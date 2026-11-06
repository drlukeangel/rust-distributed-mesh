//! The node command protocol on op `0x22` (i144 R22; Luke's rulings Q6 and Q7, 2026-10-16): the
//! mesh-primary's command to a node of its own mesh, answered on that command's own ordered reply
//! stream, so the node reports what it accepted, the state it entered and its completion, in order,
//! on one channel. Server-streaming and never forwarded. `StopNode` and `StartNode` take two hops,
//! each one direct call (D40): the caller's to the node's mesh-primary (the fabric-primary for a
//! mesh-primary), which admits and runs the node command, never a Build or part of one (Q342), and
//! the mesh-primary's to the exact node, whose frames it writes as the node's status and sends on as
//! live frames of the caller's stream. Nothing checks who sent a command (node-rpc.md §14).
//!
//! `node.connection.delete` (node-connections-delete.md) is its first command:
//!
//! ```text
//! DeleteConnections { All }
//!   <- CommandAccepted        Started: the node validated the command; no lifecycle state
//!   <- ConnectionsDeleting    Data: the node's block on new application work is in force
//!   <- ConnectionsDeleted     Terminal: Established or Deadline
//! DeleteConnections { Connections([ids]) }
//!   <- CommandAccepted        Started; no node-wide state
//!   <- ConnectionsDeleted     Terminal
//! refused before acceptance: one refusal frame
//! ```
//!
//! Q47 appends `StopNode` and `StartNode`; Q205 makes the stop a workflow that calls its primitives
//! and streams each one's acknowledgement and completion: stop reports `CommandAccepted`, then
//! `ConnectionsDeleting` and `StopConnectionsDeleted` (node.connection.delete's, Data), then
//! `Leaving` and `StopMeshLeft` (node.mesh.leave's, Data), then `NodeStopped` (Terminal). node.mesh.leave
//! is a step of the stop only, never a command of its own (Q345): request position 4 and reply
//! positions 21 and 22 are reserved. Start reports `CommandAccepted`,
//! `NodeJoined` (Data), then `NodeStarted` (Terminal), or a typed `Failed` terminal after acceptance.
//! A lifecycle state refusal before acceptance is `RejectedInvalidNodeTransition`.
//! `CommandAccepted` carries no lifecycle state: on a stop, its admission records stopping.
//! `ConnectionsDeleting`, `StopConnectionsDeleted`, `Leaving`, `StopMeshLeft`, `NodeJoined`,
//! `NodeStopped` and `NodeStarted` record their own effects. A stream that breaks after
//! `CommandAccepted` is incomplete: whatever the frames before the break reported stands, and no
//! completion is claimed. The record is of the transport header's `(invocation_id, seq)`: delivery
//! orders and suppresses repeats before handing progress to the mesh-primary, which records it
//! before acknowledging recovery. This adds no identity to the body and no receipt on the node.
//! A Build reissuing an uncertain DeleteConnections step opens a new invocation: the node executes
//! the naturally idempotent closure again. Completed steps come from the Build's step receipts.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::status::{ConnectionScope, ConnectionsReceipt, WhichMesh, MAX_CONNECTION_IDS};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{ConnectionId, IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The node command protocol on op `0x22`.
#[derive(Debug, Clone, Copy)]
pub struct NodeCommand;

/// A command to the exact birth it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandRequest {
    /// `node.connection.delete` (R-CR3): the exact birth hard-cuts the connections the scope
    /// names, writing each drop under the name it holds. `All` is graceful node-wide closure: the
    /// birth blocks new application work, lets admitted work finish under its bound, then closes
    /// every open connection (node-connections-delete.md step 3). Its endpoint stays bound and the
    /// call's own stream stays open.
    ///
    /// Position 0 on the wire.
    DeleteConnections {
        /// The birth whose connections are cut.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// Which connections.
        scope: ConnectionScope,
    },
    /// Stop this exact birth, reporting closure and completion on this stream (Q47).
    ///
    /// Position 1 on the wire.
    StopNode {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The command's identity, keyed on the node.id.
        operation: crate::OperationId,
    },
    /// Start this stopped exact birth, reporting join and readiness on this stream (Q47).
    ///
    /// Position 2 on the wire.
    StartNode {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The command's identity, keyed on the node.id.
        operation: crate::OperationId,
    },
    /// A storage change's move of the exact birth to the provider its change decided (storage.md §2
    /// "Changing it", step 4: "Each mesh-primary moves its nodes to the new provider"), carried by its
    /// mesh-primary: answered [`NodeCommandReply::StorageStepped`] once the birth writes `provider`, or
    /// [`NodeCommandReply::RejectedStaleChange`] for a command of a change older than the one it acted on.
    ///
    /// Position 3 on the wire.
    StorageStep {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The change.
        change_id: crate::ChangeId,
        /// The provider the birth writes from here on.
        provider: String,
    },
    /// Reserved position (Q345): never sent; node.mesh.leave is a step of node.stop only. A receiver
    /// refuses it by name and executes nothing; the position and its fields stay so no later variant
    /// moves.
    ///
    /// Position 4 on the wire.
    Reserved4 {
        /// Never read.
        node_id: NodeId,
        /// Never read.
        incarnation: IncarnationId,
        /// Never read.
        operation: crate::OperationId,
    },
    /// Freeze this exact birth's property writer before a storage copy.
    ///
    /// Position 5 on the wire.
    StorageBlock {
        /// The commanded node.
        node_id: NodeId,
        /// The commanded incarnation.
        incarnation: IncarnationId,
        /// The exact change whose move or rollback releases this block.
        change_id: crate::ChangeId,
    },
    /// `mesh.block` (storage.md §2 "Changing it", step 1): the fabric-primary's command to the exact
    /// birth of a mesh's mesh-primary, a step of the fabric-primary's own storage change, never a
    /// Build. The mesh-primary waits until its in-flight commands complete and every node of its mesh
    /// has frozen its writes, then answers [`NodeCommandReply::MeshBlocked`] and issues no new command
    /// until it is unblocked. Appended: no variant before it moves.
    ///
    /// Position 6 on the wire.
    BlockMesh {
        /// The mesh-primary's node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The change.
        change_id: crate::ChangeId,
        /// The provider the fabric writes at the change's start: the old provider.
        from: String,
        /// The provider the change names: the new provider.
        to: String,
    },
    /// `mesh.unblock` (storage.md §2 "Changing it", step 4): the fabric-primary's command to the exact
    /// birth of a mesh's mesh-primary. The mesh-primary moves itself and every node of its mesh onto
    /// the provider the change's `rdm.storage.committed` event decided (`to` when the event exists,
    /// `from` when it does not), answers [`NodeCommandReply::MeshUnblocked`] naming it, and issues
    /// commands again. Appended: no variant before it moves.
    ///
    /// Position 7 on the wire.
    UnblockMesh {
        /// The mesh-primary's node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The change.
        change_id: crate::ChangeId,
        /// The old provider.
        from: String,
        /// The new provider.
        to: String,
    },
    /// `node.config.update` (node-config-update.md, Q367): put the exact birth's whole config. The
    /// caller's command goes to the fabric-primary, which sends it to the node's mesh-primary, which
    /// sends it to the node; the node writes the map on its own record (Q356). Its `CommandAccepted` is
    /// `node.config.updating`, and the stream ends with [`NodeCommandReply::PropertyUpdated`].
    ///
    /// Position 8 on the wire.
    UpdateConfig {
        /// The birth whose config is put.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The whole config: a put replaces, it never merges.
        config: std::collections::BTreeMap<String, String>,
    },
    /// `node.name.update` (node-name-update.md, Q367): rename the exact birth. The same hops as
    /// [`Self::UpdateConfig`]; the node writes the name on its own record (Q356). It carries the name
    /// only: a node's path never moves. Its `CommandAccepted` is `node.name.updating`, and the stream
    /// ends with [`NodeCommandReply::PropertyUpdated`].
    ///
    /// Position 9 on the wire.
    UpdateName {
        /// The birth renamed.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The new name.
        name: String,
    },
}

impl NodeCommandRequest {
    /// The command's name in spans and evidence.
    pub fn op(&self) -> &'static str {
        match self {
            Self::DeleteConnections { .. } => "delete-connections",
            Self::StopNode { .. } => "stop-node",
            Self::StartNode { .. } => "start-node",
            Self::StorageStep { .. } => "storage-step",
            Self::StorageBlock { .. } => "storage-block",
            Self::Reserved4 { .. } => "reserved-4",
            Self::BlockMesh { .. } => "block-mesh",
            Self::UnblockMesh { .. } => "unblock-mesh",
            Self::UpdateConfig { .. } => "node.config.update",
            Self::UpdateName { .. } => "node.name.update",
        }
    }

    /// The birth the command names.
    pub fn subject(&self) -> (&NodeId, &IncarnationId) {
        match self {
            Self::DeleteConnections { node_id, incarnation, .. } | Self::StopNode { node_id, incarnation, .. } | Self::StartNode { node_id, incarnation, .. } | Self::StorageBlock { node_id, incarnation, .. } | Self::StorageStep { node_id, incarnation, .. } | Self::Reserved4 { node_id, incarnation, .. } | Self::BlockMesh { node_id, incarnation, .. } | Self::UnblockMesh { node_id, incarnation, .. } | Self::UpdateConfig { node_id, incarnation, .. } | Self::UpdateName { node_id, incarnation, .. } => (node_id, incarnation),
        }
    }
}

/// One reply frame of a node command's stream. Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandReply {
    /// The node validated the command and runs it: the stream's first frame. It carries no
    /// lifecycle state.
    ///
    /// Position 0 on the wire.
    CommandAccepted {
        /// The birth that accepted it.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// `All` only: the node's block on new application work is in force; it is connections-deleting
    /// until its completion. The state the mesh-primary records and publishes for the birth.
    ///
    /// Position 1 on the wire.
    ConnectionsDeleting {
        /// The birth that entered connections-deleting.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// `node.connections.deleted`: the cut is complete. Every connection named is accounted for by
    /// its entry id: `closed` were open and are cut, their drops written under the name the birth
    /// holds; `not_open` were named and are unknown or already closed; `held` are the connections
    /// the birth still holds after the cut (none after an `Established` `All`).
    ///
    /// Position 2 on the wire.
    ConnectionsDeleted {
        /// The open entries cut.
        closed: Vec<ConnectionId>,
        /// The ids named that are no open entry of the birth: unknown, or already closed.
        not_open: Vec<ConnectionId>,
        /// The open entries the birth still holds.
        held: Vec<ConnectionId>,
        /// For `All`, what the closure established; `None` for `Connections`.
        receipt: Option<ConnectionsReceipt>,
    },
    /// The scope names nothing the birth can cut (an empty list, or more ids than one request
    /// carries); nothing is cut.
    ///
    /// Position 3 on the wire.
    RejectedConnectionScope {
        /// Why, by name.
        reason: String,
    },
    /// The command names another birth than the one it reached: the incarnation this one holds.
    ///
    /// Position 4 on the wire.
    RejectedStaleIncarnation {
        /// The incarnation the receiver holds.
        held: IncarnationId,
    },
    /// The receiver cannot take the command yet (no connections writer, its server not ready).
    ///
    /// Position 5 on the wire.
    NotReady {
        /// Why, by name.
        reason: String,
    },
    /// The receiver could not resolve the peer the call came over.
    ///
    /// Position 6 on the wire.
    PeerUnresolved {
        /// Why, by name.
        reason: String,
    },
    /// The receiver is at its admission bound for the op.
    ///
    /// Position 7 on the wire.
    Busy {
        /// Why, by name.
        reason: String,
    },
    /// The receiver refuses new non-status calls while connections-deleting. Never sent for this op,
    /// which a connections-deleting node serves (a second `All` reaches it, step 5).
    ///
    /// Position 8 on the wire.
    RejectedConnectionsDeleting {
        /// Why, by name.
        reason: String,
    },
    /// The request did not decode or broke the protocol's bounds.
    ///
    /// Position 9 on the wire.
    Malformed {
        /// What was malformed.
        kind: MalformedKind,
    },
    /// The caller is not authorized for the op.
    ///
    /// Position 10 on the wire.
    Unauthorized {
        /// Why, by name.
        reason: String,
    },
    /// The stop's whole All closure; progress, never the stop's terminal.
    ///
    /// Position 11 on the wire.
    StopConnectionsDeleted {
        /// The closure's Established or Deadline evidence.
        receipt: ConnectionsReceipt,
    },
    /// The stop's transport cut is complete; the live process is stopped.
    ///
    /// Position 12 on the wire.
    NodeStopped,
    /// The accepted join and channel join completed; start gates remain.
    ///
    /// Position 13 on the wire.
    NodeJoined,
    /// Every start gate completed; the receiver records Ready.
    ///
    /// Position 14 on the wire.
    NodeStarted,
    /// An accepted command failed at this named step.
    ///
    /// Position 15 on the wire.
    Failed {
        /// The step that failed.
        step: String,
        /// Its explanation.
        reason: String,
        /// The step's typed outcome code.
        code: crate::outcome::Code,
    },
    /// The lifecycle command is invalid for this living birth's held state.
    ///
    /// Position 16 on the wire.
    RejectedInvalidNodeTransition {
        /// The state it holds, named by the refusal.
        current: crate::status::NodeState,
    },
    /// The birth writes `provider`: it moved onto it, or wrote it already. The command's terminal.
    ///
    /// Position 17 on the wire.
    StorageStepped {
        /// The provider the birth writes.
        provider: String,
    },
    /// The command names a storage change older than the one the birth acted on: refused by name,
    /// nothing done (storage.md §2 "Changing it").
    ///
    /// Position 18 on the wire.
    RejectedStaleChange {
        /// The change the birth acted on last.
        held: crate::ChangeId,
    },
    /// Inside a stop, node.mesh.leave's acknowledgement (Q205): the birth is leaving its mesh; the
    /// state the mesh-primary writes on its row. Appended, as are the three below: no variant before
    /// them moves.
    ///
    /// Position 19 on the wire.
    Leaving {
        /// The birth that is leaving.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// Inside a stop, node.mesh.leave's completion, `node.mesh.left`: progress, never the stop's
    /// terminal.
    ///
    /// Position 20 on the wire.
    StopMeshLeft {
        /// Which meshes the birth left: its own, and the backbone for a node-admin.
        left: Vec<WhichMesh>,
    },
    /// Reserved position (Q345): never sent.
    ///
    /// Position 21 on the wire.
    Reserved21 {
        /// Never read.
        node_id: NodeId,
        /// Never read.
        incarnation: IncarnationId,
        /// Never read.
        left: Vec<WhichMesh>,
    },
    /// Reserved position (Q345): never sent.
    ///
    /// Position 22 on the wire.
    Reserved22 {
        /// Never read.
        connections: Vec<ConnectionId>,
    },
    /// Property admission is closed and earlier writes are finished for this change.
    ///
    /// Position 23 on the wire.
    StorageBlocked,
    /// `blocked`: the mesh-primary's answer to [`NodeCommandRequest::BlockMesh`], the command's
    /// terminal. Appended: no variant before it moves.
    ///
    /// Position 24 on the wire.
    MeshBlocked,
    /// `unblocked`: the mesh-primary's answer to [`NodeCommandRequest::UnblockMesh`], the command's
    /// terminal: every node of its mesh writes `provider`. Appended: no variant before it moves.
    ///
    /// Position 25 on the wire.
    MeshUnblocked {
        /// The provider the change decided, which every node of the mesh writes.
        provider: String,
    },
    /// A caller's `StopNode` or `StartNode` refused by the primary above the node before anything is
    /// sent to the node, by name (node-stop.md, node-start.md "Entry"): `node-not-live` (a stop needs
    /// a live birth, a start a stopped one), `no-secondary` (a primary's stop has no successor),
    /// `receiver-not-primary` (the receiver is not the primary above the node). Nothing is written.
    /// Appended: no variant before it moves.
    ///
    /// Position 26 on the wire.
    Rejected {
        /// The refusal's name.
        reason: String,
        /// What it refused, for the reader.
        detail: String,
    },
    /// The terminal of an accepted [`NodeCommandRequest::UpdateConfig`] or
    /// [`NodeCommandRequest::UpdateName`]: `node.config.updated` or `node.name.updated`, or how the
    /// command ended short (`ConfigNotApplied`, `NameNotApplied`, the node's own refusal). The reply is
    /// the one vocabulary of a node's property commands, [`crate::status::StatusReply`]. Appended: no
    /// variant before it moves.
    ///
    /// Position 27 on the wire.
    PropertyUpdated {
        /// How the command ended.
        reply: crate::status::StatusReply,
    },
    /// An [`NodeCommandRequest::UpdateConfig`] or [`NodeCommandRequest::UpdateName`] refused before
    /// anything was accepted, by name (`NameHeld`, `RejectedName`, `RejectedAdmittingConnections`,
    /// `RejectedNotPrimary`, a stale birth). Nothing was sent to the node. Appended: no variant before
    /// it moves.
    ///
    /// Position 28 on the wire.
    PropertyRefused {
        /// The refusal.
        reply: crate::status::StatusReply,
    },
}

impl NodeCommandReply {
    /// How a property command ended, in the one vocabulary of a node's property commands: the reply
    /// [`Self::PropertyUpdated`] or [`Self::PropertyRefused`] carries, or the same refusal a receiver
    /// answers any node command with before it knows the command.
    pub fn property_end(self) -> crate::status::StatusReply {
        use crate::reply::status as r;
        match self {
            Self::PropertyUpdated { reply } | Self::PropertyRefused { reply } => reply,
            Self::NotReady { reason } => r::not_ready(reason),
            Self::PeerUnresolved { reason } => r::peer_unresolved(reason),
            Self::Busy { reason } => r::busy(reason),
            Self::RejectedConnectionsDeleting { reason } => r::connections_deleting(reason),
            Self::Malformed { kind } => r::malformed(kind),
            Self::RejectedStaleIncarnation { held } => r::rejected_stale_incarnation(held),
            other => r::request_not_handled(other.name().to_string()),
        }
    }
}

impl NodeCommandReply {
    /// The reply's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CommandAccepted { .. } => "command-accepted",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::ConnectionsDeleted { .. } => "connections-deleted",
            Self::RejectedConnectionScope { .. } => "rejected-connection-scope",
            Self::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            Self::NotReady { .. } => "not-ready",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::RejectedConnectionsDeleting { .. } => "rejected-connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::StopConnectionsDeleted { .. } => "stop-connections-deleted",
            Self::NodeStopped => "node-stopped",
            Self::NodeJoined => "node-joined",
            Self::NodeStarted => "node-started",
            Self::Failed { .. } => "failed",
            Self::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            Self::StorageStepped { .. } => "storage-stepped",
            Self::StorageBlocked => "storage-blocked",
            Self::RejectedStaleChange { .. } => "rejected-stale-change",
            Self::Leaving { .. } => "leaving",
            Self::StopMeshLeft { .. } => "stop-mesh-left",
            Self::Reserved21 { .. } => "reserved-21",
            Self::Reserved22 { .. } => "reserved-22",
            Self::MeshBlocked => "mesh-blocked",
            Self::MeshUnblocked { .. } => "mesh-unblocked",
            Self::Rejected { .. } => "rejected",
            Self::PropertyUpdated { .. } => "property-updated",
            Self::PropertyRefused { .. } => "property-refused",
        }
    }
}

impl NodeCommandReply {
    /// The per-element code view of a served named-connection answer, shared with Status.
    pub fn connection_outcomes(&self) -> Vec<crate::outcome::ElementOutcome<'_, crate::ConnectionId>> {
        match self {
            Self::ConnectionsDeleted { closed, not_open, .. } => crate::outcome::connection_outcomes(closed, not_open),
            _ => Vec::new(),
        }
    }
}

impl NodeProtocol for NodeCommand {
    const OP: u8 = 0x22;
    const NAME: &'static str = "node-command";
    /// A command naming the most connection ids one request carries ([`MAX_CONNECTION_IDS`]).
    const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024;
    /// A `node.connections.deleted` names every connection it accounts for by id
    /// ([`crate::status::MAX_REPORTED_CONNECTIONS`] of them).
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// Each hop is one direct call: never carried by a peer.
    const FORWARDABLE: bool = false;
    /// A connections-deleting node still takes a command: a second `All` leaves an incomplete
    /// closure (node-connections-delete.md step 5).
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 10;
    const REPLY_VARIANTS: u32 = 29;

    type Request = NodeCommandRequest;
    type Reply = NodeCommandReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            NodeCommandReply::CommandAccepted { .. } => "command-accepted",
            NodeCommandReply::ConnectionsDeleting { .. } => "connections-deleting",
            NodeCommandReply::ConnectionsDeleted { .. } => "connections-deleted",
            NodeCommandReply::RejectedConnectionScope { .. } => "rejected-connection-scope",
            NodeCommandReply::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            NodeCommandReply::NotReady { .. } => "not-ready",
            NodeCommandReply::PeerUnresolved { .. } => "peer-unresolved",
            NodeCommandReply::Busy { .. } => "busy",
            NodeCommandReply::RejectedConnectionsDeleting { .. } => "rejected-connections-deleting",
            NodeCommandReply::Malformed { .. } => "malformed",
            NodeCommandReply::Unauthorized { .. } => "unauthorized",
            NodeCommandReply::StopConnectionsDeleted { .. } => "stop-connections-deleted",
            NodeCommandReply::NodeStopped => "node-stopped",
            NodeCommandReply::NodeJoined => "node-joined",
            NodeCommandReply::NodeStarted => "node-started",
            NodeCommandReply::Failed { .. } => "failed",
            NodeCommandReply::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            NodeCommandReply::StorageStepped { .. } => "storage-stepped",
            NodeCommandReply::StorageBlocked => "storage-blocked",
            NodeCommandReply::RejectedStaleChange { .. } => "rejected-stale-change",
            NodeCommandReply::Leaving { .. } => "leaving",
            NodeCommandReply::StopMeshLeft { .. } => "stop-mesh-left",
            NodeCommandReply::Reserved21 { .. } => "reserved-21",
            NodeCommandReply::Reserved22 { .. } => "reserved-22",
            NodeCommandReply::MeshBlocked => "mesh-blocked",
            NodeCommandReply::MeshUnblocked { .. } => "mesh-unblocked",
            NodeCommandReply::Rejected { .. } => "rejected",
            NodeCommandReply::PropertyUpdated { .. } => "property-updated",
            NodeCommandReply::PropertyRefused { .. } => "property-refused",
        }
    }

    fn classify_reply(reply: &NodeCommandReply) -> ReplyKind {
        match reply {
            NodeCommandReply::CommandAccepted { .. } | NodeCommandReply::NodeStopped | NodeCommandReply::NodeJoined | NodeCommandReply::NodeStarted | NodeCommandReply::StorageStepped { .. } | NodeCommandReply::StorageBlocked | NodeCommandReply::MeshBlocked | NodeCommandReply::MeshUnblocked { .. } => ReplyKind::Success,
            // The reserved positions keep the codes their frozen positions carry.
            NodeCommandReply::Leaving { .. } | NodeCommandReply::StopMeshLeft { .. } | NodeCommandReply::Reserved21 { .. } => ReplyKind::Success,
            NodeCommandReply::Reserved22 { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
            NodeCommandReply::RejectedStaleChange { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodeCommandReply::Failed { code, .. } => ReplyKind::new(*code),
            NodeCommandReply::RejectedInvalidNodeTransition { .. } | NodeCommandReply::Rejected { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodeCommandReply::StopConnectionsDeleted { receipt: ConnectionsReceipt::Deadline { .. } } => ReplyKind::new(crate::outcome::Code::DeadlineExceeded),
            NodeCommandReply::StopConnectionsDeleted { .. } => ReplyKind::Success,
            NodeCommandReply::ConnectionsDeleting { .. } => ReplyKind::Success,
            NodeCommandReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Deadline { .. }), .. } => ReplyKind::new(crate::outcome::Code::DeadlineExceeded),
            NodeCommandReply::ConnectionsDeleted { .. } => ReplyKind::Success,
            NodeCommandReply::RejectedConnectionScope { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            NodeCommandReply::RejectedStaleIncarnation { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodeCommandReply::NotReady { .. } => ReplyKind::NotReady,
            NodeCommandReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            NodeCommandReply::Busy { .. } => ReplyKind::Busy,
            NodeCommandReply::RejectedConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            NodeCommandReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            NodeCommandReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            NodeCommandReply::PropertyUpdated { reply } | NodeCommandReply::PropertyRefused { reply } => crate::status::Status::classify_reply(reply),
        }
    }

    fn peer_unresolved(reason: String) -> NodeCommandReply {
        NodeCommandReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> NodeCommandReply {
        NodeCommandReply::NotReady { reason }
    }
    fn busy(reason: String) -> NodeCommandReply {
        NodeCommandReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> NodeCommandReply {
        NodeCommandReply::RejectedConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> NodeCommandReply {
        NodeCommandReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> NodeCommandReply {
        NodeCommandReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for NodeCommand {
    fn frame_kind(frame: &NodeCommandReply) -> FrameKind {
        match frame {
            NodeCommandReply::CommandAccepted { .. } => FrameKind::Started,
            NodeCommandReply::ConnectionsDeleting { .. } | NodeCommandReply::StopConnectionsDeleted { .. } | NodeCommandReply::Leaving { .. } | NodeCommandReply::StopMeshLeft { .. } | NodeCommandReply::NodeJoined => FrameKind::Data,
            NodeCommandReply::PropertyUpdated { .. } => FrameKind::Terminal,
            NodeCommandReply::ConnectionsDeleted { .. } | NodeCommandReply::Reserved21 { .. } | NodeCommandReply::NodeStopped | NodeCommandReply::NodeStarted | NodeCommandReply::Failed { .. } | NodeCommandReply::StorageStepped { .. } | NodeCommandReply::StorageBlocked | NodeCommandReply::MeshBlocked | NodeCommandReply::MeshUnblocked { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(NodeCommand::classify_reply(other)),
        }
    }
}

const _: () = assert!(MAX_CONNECTION_IDS * (1 + crate::CROCKFORD60_LEN) + 64 <= NodeCommand::MAX_REQUEST_FRAME_BYTES, "a DeleteConnections naming the most ids one request carries fits the request bound");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::FrameOrder;

    fn accepted() -> NodeCommandReply {
        NodeCommandReply::CommandAccepted { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }
    }

    fn deleted(receipt: Option<ConnectionsReceipt>) -> NodeCommandReply {
        NodeCommandReply::ConnectionsDeleted { closed: vec![ConnectionId::mint()], not_open: vec![], held: vec![], receipt }
    }

    /// CONTRACT (Luke Q7: "Replies (NodeCommand replies, classified into the existing stream order)"):
    /// `All` streams `CommandAccepted`, `ConnectionsDeleting`, then `ConnectionsDeleted`;
    /// `Connections([ids])` streams `CommandAccepted` then `ConnectionsDeleted`; a refusal before
    /// acceptance is one frame. Each is a legal order of the transport's existing enforcer. Must NOT
    /// happen: a progress frame before acceptance, a refusal after it, or a second completion.
    // @feature: node-lifecycle
    #[test]
    fn both_scopes_follow_the_stream_order_and_a_refusal_is_one_frame() {
        let mut all = FrameOrder::new();
        for f in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, deleted(Some(ConnectionsReceipt::Established { in_flight: 0, closed: vec![] }))] {
            all.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("All: {f:?}: {e}"));
        }
        let mut some = FrameOrder::new();
        for f in [accepted(), deleted(None)] {
            some.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("Connections: {f:?}: {e}"));
        }
        for refusal in [NodeCommandReply::RejectedConnectionScope { reason: "empty".into() }, NodeCommandReply::RejectedStaleIncarnation { held: IncarnationId::mint() }, NodeCommandReply::NotReady { reason: "not yet".into() }] {
            let mut one = FrameOrder::new();
            one.admit(NodeCommand::frame_kind(&refusal)).unwrap_or_else(|e| panic!("{refusal:?} alone: {e}"));
            assert!(one.admit(NodeCommand::frame_kind(&accepted())).is_err(), "nothing follows a refusal");
        }
        let mut early = FrameOrder::new();
        assert!(early.admit(NodeCommand::frame_kind(&NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() })).is_err(), "no progress before acceptance");
        let mut late = FrameOrder::new();
        late.admit(NodeCommand::frame_kind(&accepted())).unwrap();
        assert!(late.admit(NodeCommand::frame_kind(&NodeCommandReply::RejectedConnectionScope { reason: "late".into() })).is_err(), "no refusal after acceptance");
    }

    /// CONTRACT (Q205; node-stop.md steps 1-4): a stop streams each of its primitives' acknowledgement
    /// and completion, `CommandAccepted`, `ConnectionsDeleting`, `StopConnectionsDeleted`, `Leaving`,
    /// `StopMeshLeft`, `NodeStopped`, a legal order ending in its one terminal. Must NOT happen: a
    /// primitive's completion inside a stop ending the stop's stream.
    // @feature: node-lifecycle
    #[test]
    fn a_stop_streams_each_primitives_frames() {
        let mut stop = FrameOrder::new();
        let receipt = ConnectionsReceipt::Established { in_flight: 0, closed: vec![] };
        for f in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopConnectionsDeleted { receipt }, NodeCommandReply::Leaving { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopMeshLeft { left: vec![WhichMesh::Own] }, NodeCommandReply::NodeStopped] {
            stop.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("stop: {f:?}: {e}"));
        }
        assert!(stop.admit(NodeCommand::frame_kind(&NodeCommandReply::NodeStopped)).is_err(), "one terminal");
        for f in [NodeCommandReply::Leaving { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopMeshLeft { left: vec![WhichMesh::Own] }] {
            assert_eq!(NodeCommand::decode_reply(&NodeCommand::encode_reply(&f).unwrap()).unwrap(), f, "{}", f.name());
        }
    }

    /// CONTRACT (node-rpc-envelope.md, the wire is postcard): every request and reply of op 0x22
    /// round-trips, and a `node.connections.deleted` naming the most ids one reply carries fits the
    /// reply bound.
    // @feature: node-lifecycle
    #[test]
    fn every_node_command_frame_round_trips_under_its_bounds() {
        let req = NodeCommandRequest::DeleteConnections { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: ConnectionScope::Connections((0..MAX_CONNECTION_IDS).map(|_| ConnectionId::mint()).collect()) };
        let bytes = NodeCommand::encode_request(&req).unwrap();
        assert!(bytes.len() <= NodeCommand::MAX_REQUEST_FRAME_BYTES, "{} bytes", bytes.len());
        assert_eq!(NodeCommand::decode_request(&bytes).unwrap(), req);
        let ids = |n: usize| (0..n).map(|_| ConnectionId::mint()).collect::<Vec<_>>();
        let big = NodeCommandReply::ConnectionsDeleted { closed: ids(crate::status::MAX_REPORTED_CONNECTIONS - 2), not_open: ids(1), held: ids(1), receipt: None };
        for reply in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, big, deleted(Some(ConnectionsReceipt::Deadline { last_in_flight: 1, closed: ids(1), held: ids(1) }))] {
            let bytes = NodeCommand::encode_reply(&reply).unwrap();
            assert!(bytes.len() <= NodeCommand::MAX_REPLY_FRAME_BYTES, "{}: {} bytes", reply.name(), bytes.len());
            assert_eq!(NodeCommand::decode_reply(&bytes).unwrap(), reply);
        }
    }

    /// CONTRACT (storage.md §2 "Changing it", steps 1 and 4: "The fabric-primary sends `mesh.block` to
    /// every mesh-primary" ... "The fabric-primary sends `mesh.unblock` to every mesh-primary"; a
    /// storage change is the fabric-primary's own workflow, never a Build): `BlockMesh` and
    /// `UnblockMesh` round-trip at positions 6 and 7, carrying the change and its two providers and no
    /// Build; each streams `CommandAccepted` then its one terminal, `MeshBlocked` or `MeshUnblocked`
    /// naming the provider its mesh writes. Must NOT happen: a frame after the terminal.
    // @feature: topology-storage-provider
    #[test]
    fn a_mesh_block_and_a_mesh_unblock_stream_their_acceptance_then_their_one_terminal() {
        let change_id = crate::ChangeId::mint();
        for (req, terminal, position) in [
            (NodeCommandRequest::BlockMesh { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), change_id: change_id.clone(), from: "file".into(), to: "test-target".into() }, NodeCommandReply::MeshBlocked, 6u8),
            (NodeCommandRequest::UnblockMesh { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), change_id: change_id.clone(), from: "file".into(), to: "test-target".into() }, NodeCommandReply::MeshUnblocked { provider: "test-target".into() }, 7u8),
        ] {
            let bytes = NodeCommand::encode_request(&req).unwrap();
            assert_eq!(bytes[0], position, "{} is appended at position {position}", req.op());
            assert_eq!(NodeCommand::decode_request(&bytes).unwrap(), req);
            assert_eq!(NodeCommand::decode_reply(&NodeCommand::encode_reply(&terminal).unwrap()).unwrap(), terminal);
            let mut order = FrameOrder::new();
            for f in [accepted(), terminal.clone()] {
                order.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("{}: {f:?}: {e}", req.op()));
            }
            assert!(order.admit(NodeCommand::frame_kind(&terminal)).is_err(), "{}: one terminal", req.op());
        }
    }

    /// CONTRACT (node-rpc-envelope.md, NodeCommand op 0x22: `StorageBlock`, `BlockMesh` and
    /// `UnblockMesh` each stream `CommandAccepted` then their terminal or `Failed { step, reason, code }`;
    /// "A refusal before acceptance is one refusal frame"): once accepted, a storage command's not-ready
    /// answer ends its stream `Failed` at its step, `UNAVAILABLE`, and a stale change `Failed`, `ABORTED`,
    /// naming the change; a success terminal is as it is. Must NOT happen: a refusal frame after
    /// `CommandAccepted`, which the stream refuses as a protocol violation (424).
    // @feature: topology-storage-provider
    #[test]
    fn an_accepted_storage_commands_refusal_ends_its_stream_failed_at_its_step() {
        use crate::outcome::Code;
        let held = crate::ChangeId::mint();
        for step in ["storage-block", "mesh.block", "mesh.unblock"] {
            for (refusal, code) in [(crate::reply::node_command::not_ready("a node of this mesh has not frozen its writes yet".into()), Code::Unavailable), (crate::reply::node_command::rejected_stale_change(held.clone()), Code::Aborted)] {
                let mut order = FrameOrder::new();
                order.admit(NodeCommand::frame_kind(&accepted())).unwrap();
                let mut refused = FrameOrder::new();
                refused.admit(NodeCommand::frame_kind(&accepted())).unwrap();
                assert!(refused.admit(NodeCommand::frame_kind(&refusal)).is_err(), "{step}: a refusal after the acceptance is a protocol violation: {refusal:?}");
                let ended = crate::reply::node_command::after_acceptance(step, refusal.clone());
                assert!(matches!(&ended, NodeCommandReply::Failed { step: s, code: c, .. } if s == step && *c == code), "{step}: {refusal:?} ends Failed at its step, {code:?}: {ended:?}");
                order.admit(NodeCommand::frame_kind(&ended)).unwrap_or_else(|e| panic!("{step}: Failed is the accepted stream's terminal: {e}"));
            }
            for done in [NodeCommandReply::StorageBlocked, NodeCommandReply::MeshBlocked, NodeCommandReply::MeshUnblocked { provider: "test-target".into() }] {
                assert_eq!(crate::reply::node_command::after_acceptance(step, done.clone()), done, "{step}: a terminal is as it is");
            }
        }
        assert!(matches!(crate::reply::node_command::after_acceptance("storage-block", crate::reply::node_command::rejected_stale_change(held.clone())), NodeCommandReply::Failed { reason, .. } if reason.contains(held.as_str())), "the stale change is named");
    }
}
