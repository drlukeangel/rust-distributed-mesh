//! The gossip frames (gossip.md §3.1): the `Frame` enum, its wire positions and the forwarding
//! a receiving primary applies to a frame.


use crate::snapshot::PublisherId;
use anyhow::Result;
use rdm_core::{FabricId, IncarnationId, LifecycleOp, MeshDigest, MeshId, NodeId, Primary, PrimaryRole, RoleRevision, StateHook};
use rdm_core::model::NodeStatus;
use rdm_core::framing::Birth;
use rdm_core::wire::{NodeDetails, NodeEntry, NodeStatusEntry};
use serde::{Deserialize, Serialize};



/// The largest encoded frame: the payload that fits iroh-gossip's frame limit with its framing.
pub const MAX_FRAME: usize = rdm_transport::chunking::MAX_MESSAGE_BYTES;

/// What travels on a mesh channel or the backbone: one postcard frame ([`rdm_transport::wire`]). A digest
/// travels as its wire shape (`#[serde(with)]`), never as the JSON-shaped `MeshDigest`.
///
/// The complete gossip message is postcard `(Vec<(String, String)>, Vec<u8>)`: standard CloudEvents
/// metadata followed by opaque data selected by its literal type. The metadata names are
/// `specversion`, `id`, `source`, `type`, `subject`, optional `sequence`, `time`, and
/// `datacontenttype`; `source` names the original author birth. A variant describes domain data, its
/// fields in declaration order (their postcard position), without an outer kind discriminant on the
/// wire: a variant's schema index is internal, never a wire kind. The in-process carrier
/// [`Frame::Authored`] is never serialized. A `TopologyFull`'s author, version and time are its
/// envelope's alone (`source`, `sequence`, `time`; [`Frame::authored_aggregate`]). A field with a serde
/// `with` module travels in that module's wire shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    /// A node's own Digest (its mesh channel only; gossip.md §3.1 `Digest`): its status and the
    /// primary roles it holds, at once when its status changes and on its heartbeat. The envelope's
    /// `source` is the exact birth, its `sequence` the birth's `digest_seq` and its `time` the emitted
    /// time; the body repeats none of them. Every Digest carries the node's `details`, so a receiver
    /// names the birth from whichever Digest of it it hears.
    ///
    /// Schema index 0, never a wire kind.
    Digest {
        /// The node's status.
        status: NodeStatus,
        /// The primary roles the node holds now, each with its revision (Q392); empty while it holds none.
        primary: Vec<RoleRevision>,
        /// The node's details: type, path, name, endpoint id, transport address and the birth it supersedes.
        details: NodeDetails,
    },
    /// One chunk of `mesh`'s aggregate (gossip.md §3.1): the mesh-primary's one list of its mesh's
    /// nodes, each with its identity, endpoints, status and primary roles, on the backbone only (Q387,
    /// Q390). A node under a stop or delete, or deleted, says so by its status; a deleted node is
    /// listed for the deleted retention, then dropped. The envelope's `source` is the mesh-primary that
    /// authored it, its `sequence` the mesh's version (Q389) and its `time` the publication's
    /// Rafka-time; its `id` is `rdm.mesh.members/<mesh>/<version>/<chunk>`. The version bumps on a
    /// material change of the list and never on a heartbeat, a stamp or an unchanged republish. The
    /// list is split into chunks only when it does not fit one message (Q390); a receiver installs it
    /// only once it holds every chunk of that (mesh, version, author).
    ///
    /// Schema index 1, never a wire kind.
    TopologyFull {
        /// The mesh's name.
        mesh: String,
        /// The mesh's id: a later mesh of the same name has another id, and the held mesh's version
        /// orders nothing of it (gossip.md "Mesh delete").
        mesh_id: Option<MeshId>,
        /// This chunk's index within the aggregate.
        chunk_index: u32,
        /// The number of chunks in the aggregate.
        chunk_count: u32,
        /// The nodes this chunk carries.
        nodes: Vec<NodeEntry>,
    },
    /// One chunk of a peer mesh's differential, published by the forwarding mesh-primary on its own
    /// mesh channel only (gossip.md §3.1, §3.3): what moved the peer mesh from `base_version`, the
    /// version that mesh-primary last published into its mesh, to the envelope's `sequence`, the peer
    /// mesh's version, whichever of its mesh-primaries sent it (Q389). The envelope's `source` is the
    /// forwarding mesh-primary; the body repeats neither. `added` are the nodes added with their
    /// details (and a held birth whose details changed), `changed` the held nodes whose status or
    /// primary roles changed, `removed` the births no longer listed, and `in_flight` and `deleted` the
    /// overlays and deletions added since `base_version`. A differential that does not fit one message
    /// is sent as `chunk_count` chunks; a receiver applies it once it holds every chunk of that (mesh,
    /// `base_version`, version, author), and only when it holds exactly `base_version` of the mesh.
    ///
    /// Schema index 2, never a wire kind.
    TopologyDelta {
        /// The peer mesh's name.
        mesh: String,
        /// The peer mesh's id, as [`Frame::TopologyFull`] carries it.
        mesh_id: Option<MeshId>,
        /// The version a receiver must hold for the differential to apply.
        base_version: u64,
        /// This chunk's index within the differential.
        chunk_index: u32,
        /// The number of chunks in the differential.
        chunk_count: u32,
        /// The nodes added, each with its details, status and primary roles.
        added: Vec<NodeEntry>,
        /// The held nodes whose status or primary roles changed.
        changed: Vec<NodeStatusEntry>,
        /// The births no longer listed.
        removed: Vec<Birth>,
        /// The overlays added since the base version.
        in_flight: Vec<LifecycleOp>,
        /// The deletions added since the base version.
        deleted: Vec<LifecycleOp>,
    },
    /// The mesh executor holding `op` has started removing its exact birth:
    /// the node is still found, and application routing stops selecting it.
    /// Published on the executor's own mesh channel only; the backbone carries the node's deleting
    /// status in its mesh's aggregate (`TopologyFull`).
    ///
    /// Schema index 3, never a wire kind.
    NodeDeleting {
        /// The removal being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The `topology_version` of the subject's mesh at which the subject changed. A receiver orders
        /// the facts of one mesh by that version (R-RX3), whichever of its mesh-primaries published
        /// them (Q389, Q391); the envelope's `source` names the one that did.
        topology_version: u64,
    },
    /// The provider proved `op`'s exact birth terminal: it has left. Same
    /// channel as `NodeDeleting`; the backbone carries the node listed deleted in its mesh's aggregate
    /// for the deleted retention (`TopologyFull`).
    ///
    /// Schema index 4, never a wire kind.
    NodeDeleted {
        /// The removal that completed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// Reserved position (Q183; gossip.md, reserved lifecycle frame positions): never sent on any
    /// channel; node.restart is removed and callers run node.stop then node.start. Its type,
    /// `rdm.node.restarting`, is refused before its data is decoded; an in-process copy is refused by
    /// name too.
    ///
    /// Schema index 5, never a wire kind.
    NodeRestarting {
        /// Never read.
        op: LifecycleOp,
        /// Never read.
        forwarded_by: Option<String>,
        /// Never read.
        publisher: PublisherId,
        /// Never read.
        topology_version: u64,
    },
    /// A mesh's status, authored by that mesh's primary alone, on its own mesh channel and the
    /// backbone; a peer mesh's primary forwards it onto its own channel (`forwarded_by`) and never
    /// restates it as its own. `changed_at_rafka_ms` is the instant of the CHANGE: all five sends
    /// of one change carry the same value (gossip.md §3.1, §3.2).
    ///
    /// Schema index 6, never a wire kind.
    MeshStatus {
        /// The mesh the status belongs to.
        mesh: String,
        /// The status.
        status: String,
        /// The mesh primary that authored the status.
        publisher: String,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The Rafka-time of the change.
        changed_at_rafka_ms: u64,
    },
    /// The fabric's status, authored by the fabric primary alone; same channels, forwarding and
    /// `changed_at_rafka_ms` as [`Frame::MeshStatus`].
    ///
    /// Schema index 7, never a wire kind.
    FabricStatus {
        /// The fabric the status belongs to.
        fabric: FabricId,
        /// The status.
        status: String,
        /// The fabric primary that authored the status.
        publisher: String,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The Rafka-time of the change.
        changed_at_rafka_ms: u64,
    },
    /// Reserved page position 10 (Luke Q23): its type is refused before decoding data. Gossip never
    /// commands an investigation; the primary watch measures the primary's silence itself.
    ///
    /// Schema index 8, never a wire kind.
    Concern {
        /// The role whose primary looks silent.
        role: PrimaryRole,
        /// The silent primary's node.
        node_id: NodeId,
        /// The silent primary's exact birth.
        incarnation: IncarnationId,
        /// The mesh primary that marked the birth silent.
        observer: String,
    },
    /// Reserved position (R-CR4; gossip.md, node-connections-deleting): never sent on any channel.
    ///
    /// Schema index 9, never a wire kind.
    NodeConnectionsDeleting {
        /// The accepted command.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// Reserved position (R-CR4; gossip.md, node-leaving): never sent on any channel.
    ///
    /// Schema index 10, never a wire kind.
    NodeLeaving {
        /// The accepted stop command.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// Reserved position (R-CR4; gossip.md, node-left): never sent on any channel.
    ///
    /// Schema index 11, never a wire kind.
    NodeLeft {
        /// The stop operation and the exact birth that left.
        op: LifecycleOp,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// Reserved position (R-CR4; gossip.md, node-connections-deleted): never sent on any channel.
    ///
    /// Schema index 12, never a wire kind.
    NodeConnectionsDeleted {
        /// The operation and the exact birth whose connections were deleted.
        op: LifecycleOp,
        /// The admin or primary that forwards the frame.
        forwarded_by: Option<String>,
    },
    /// Reserved position (R-G3; gossip.md, reserved lifecycle frame positions): never sent on any
    /// channel: a primary change rides the successor's `Digest` and the mesh-primary's `TopologyFull`
    /// projection, and a birth's contact addresses ride its `Digest` and that projection.
    ///
    /// Schema index 13, never a wire kind.
    NewFabricPrimary {
        /// The committed primary: mesh, node, incarnation and the role's revision.
        primary: Primary,
        /// The primary's endpoint key.
        endpoint_id: String,
        /// The primary's bound QUIC UDP address.
        transport_addr: std::net::SocketAddr,
    },
    /// Reserved position (Q171): never sent; a receiver refuses it by name.
    ///
    /// Schema index 14, never a wire kind.
    Reserved15 {
        /// Never read.
        hook: StateHook,
        /// Never read.
        forwarded_by: Option<String>,
    },
    /// Reserved position (Q171): never sent; a receiver refuses it by name.
    ///
    /// Schema index 15, never a wire kind.
    Reserved16 {
        /// Never read.
        hook: StateHook,
        /// Never read.
        forwarded_by: Option<String>,
    },
    /// Reserved position (Q174): never sent; a receiver refuses it by name.
    ///
    /// Schema index 16, never a wire kind.
    Reserved17 {
        /// Never read.
        hook: StateHook,
        /// Never read.
        forwarded_by: Option<String>,
    },
    /// Reserved position (Q174): never sent; a receiver refuses it by name.
    ///
    /// Schema index 17, never a wire kind.
    Reserved18 {
        /// Never read.
        hook: StateHook,
        /// Never read.
        forwarded_by: Option<String>,
    },
    /// The mesh executor holding `op` (`node.stop:<path>`) received the exact birth's `left` on the
    /// stop call's own reply: the birth is stopped (its process alive, its endpoint bound) and
    /// gossips nothing. Every node drops it from its view until the birth itself rejoins. Same
    /// channel as `NodeDeleting`; the backbone carries the node's stopped status in its mesh's
    /// aggregate (`TopologyFull`).
    ///
    /// Schema index 18, never a wire kind.
    NodeStopped {
        /// The stop being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The `topology_version` of the subject's mesh at which the subject changed, ordered as on
        /// [`Frame::NodeDeleting`]: a projection of the mesh newer than it, listing the birth, moved
        /// past the stop.
        topology_version: u64,
    },
    /// Authored CloudEvents context retained alongside the selected domain data.
    /// This is an in-process carrier, never another wire kind.
    #[serde(skip)]
    Authored {
        /// The immutable authored context.
        context: crate::envelope::Context,
        /// The catalog's selected domain data.
        frame: Box<Frame>,
    },
}

impl Frame {
    /// `d`'s Digest, authored by its exact birth at its `digest_seq` and emitted time, with its node
    /// details.
    pub fn digest(d: &MeshDigest) -> Self {
        let frame = Self::Digest { status: d.status, primary: d.primary.clone(), details: NodeDetails::of(&d.node) };
        let source = crate::envelope::Birth { fabric: d.fabric_id.clone(), node: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() };
        let context = crate::envelope::Context::digest(&source, d.digest_seq, d.emitted_at_rafka_ms);
        Self::Authored { context, frame: Box::new(frame) }
    }

    /// The node a Digest heard on `mesh`'s channel names, from the details it carries and the exact
    /// birth its envelope names: `None` for another frame or a frame with no authored context.
    pub fn digest_of(&self, mesh: &str) -> Option<MeshDigest> {
        let (Self::Digest { status, primary, details }, Some(c)) = (self.data(), self.context()) else { return None };
        let node = details.node(c.source.node.clone(), c.source.incarnation.clone(), mesh);
        Some(MeshDigest { fabric_id: c.source.fabric.clone(), node, status: *status, primary: primary.clone(), digest_seq: c.digest_seq()?, emitted_at_rafka_ms: c.time_ms })
    }

    /// The selected domain data, independent of its immutable authored context.
    pub fn data(&self) -> &Self { match self { Self::Authored { frame, .. } => frame.data(), _ => self } }
    /// Consume the carrier and retain its selected data.
    pub fn into_data(self) -> Self { match self { Self::Authored { frame, .. } => frame.into_data(), _ => self } }
    /// The original authored context, when this frame is ready for the wire.
    pub fn context(&self) -> Option<&crate::envelope::Context> { match self { Self::Authored { context, .. } => Some(context), _ => None } }
    /// A differential's chunk (`TopologyDelta` data), authored by the forwarding mesh-primary `author`
    /// at the peer mesh's `version` (the envelope's `sequence`) and `time_ms`.
    pub fn authored_delta(self, author: &crate::envelope::Birth, version: u64, time_ms: u64) -> Self {
        let context = crate::envelope::Context::delta(&self, author, version, time_ms);
        Self::Authored { context, frame: Box::new(self.into_data()) }
    }

    /// The version a `TopologyDelta` moves its mesh to: its envelope's `sequence`. `None` for another
    /// frame or one with no authored context.
    pub fn delta_version(&self) -> Option<u64> {
        matches!(self.data(), Self::TopologyDelta { .. }).then(|| self.context().and_then(|c| c.version())).flatten()
    }

    /// A `TopologyFull` chunk, authored by its mesh-primary `author` at the mesh's `version` and the
    /// publication's Rafka-time `time_ms` (gossip.md §3.1: the envelope names the author, the version and
    /// the time; the body repeats none of them).
    pub fn authored_aggregate(self, author: &crate::envelope::Birth, version: u64, time_ms: u64) -> Self {
        if self.context().is_some() { return self; }
        let context = crate::envelope::Context::aggregate(&self, author, version, time_ms);
        Self::Authored { context, frame: Box::new(self) }
    }
    /// The mesh's version a `TopologyFull` carries: its envelope's `sequence` (Q389). `None` for
    /// another frame or one with no authored context.
    pub fn aggregate_version(&self) -> Option<u64> {
        match self.data() { Self::TopologyFull { .. } => self.context()?.version(), _ => None }
    }
    /// Bind locally produced data to its explicitly supplied author's birth and held revision.
    pub fn authored(self, author: &crate::envelope::Birth, primary_revision: u64, time_ms: u64) -> Self {
        if self.context().is_some() { return self; }
        let context = crate::envelope::Context::authored(&self, author, primary_revision, time_ms);
        Self::Authored { context, frame: Box::new(self) }
    }
    pub(crate) fn with_context(self, context: Option<crate::envelope::Context>) -> Self {
        match context { Some(context) => Self::Authored { context, frame: Box::new(self) }, None => self }
    }
    pub(crate) fn kind_index(&self) -> usize {
        let encoded = rdm_transport::wire::encode(self.data()).expect("domain data serializes");
        encoded[0] as usize
    }
    /// The frame as one postcard message.
    pub fn encode(&self) -> Vec<u8> {
        match self { Self::Authored { context, frame } => crate::envelope::encode(context, frame).expect("event serializes"), _ => panic!("gossip encoding requires an explicitly supplied author birth") }
    }
    /// The frame `bytes` carry, or why they are not one.
    pub fn decode(bytes: &[u8]) -> Result<Self, rdm_transport::wire::WireError> {
        crate::envelope::decode(bytes).map_err(|e| rdm_transport::wire::WireError::new(format!("{e:?}")))
    }
}

/// `op` carrying the trace context of the envelope `f` arrived in (gossip.md §3.4): the publishing
/// executor's span, kept through every forward, so the application's span made from the operation is
/// its child (telemetry.md §3, the deletion operation). Observability only: nothing reads it to decide.
pub(crate) fn traced_by_envelope(f: &Frame, op: &LifecycleOp) -> LifecycleOp {
    let context = f.context();
    op.clone().traced(context.and_then(|c| c.traceparent.clone()), context.and_then(|c| c.tracestate.clone()))
}

/// What a peer mesh's primary puts on its own mesh channel for a lifecycle or status frame it
/// heard on the backbone: the frame, unchanged but for `forwarded_by`, or nothing. A source Mesh's
/// members are never forwarded as heard: the [`Forwarder`] derives the delta or the stripped full
/// from the two snapshots the primary holds (gossip.md §3.3). `me` is the forwarding node, `own_mesh`
/// its mesh. A status keeps its publisher and its instant: a forwarder never restates one as its
/// own, and forwards only what its author sent (a replay by a holder is for the backbone only).
pub fn forward_of(frame: Frame, me: &str, own_mesh: &str) -> Option<Frame> {
    if let Frame::Authored { context, frame } = frame {
        return forward_of(*frame, me, own_mesh).map(|frame| Frame::Authored { context, frame: Box::new(frame) });
    }
    match frame {
        Frame::NodeDeleting { op, topology_version, .. } if op.mesh != own_mesh => Some(Frame::NodeDeleting { op, forwarded_by: Some(me.to_string()), topology_version }),
        Frame::NodeDeleted { op, .. } if op.mesh != own_mesh => Some(Frame::NodeDeleted { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeStopped { op, topology_version, .. } if op.mesh != own_mesh => Some(Frame::NodeStopped { op, forwarded_by: Some(me.to_string()), topology_version }),
        Frame::NodeConnectionsDeleting { op, .. } if op.mesh != own_mesh => Some(Frame::NodeConnectionsDeleting { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeConnectionsDeleted { op, .. } if op.mesh != own_mesh => Some(Frame::NodeConnectionsDeleted { op, forwarded_by: Some(me.to_string()) }),
        Frame::MeshStatus { mesh, status, publisher, forwarded_by: None, changed_at_rafka_ms } if mesh != own_mesh => {
            Some(Frame::MeshStatus { mesh, status, publisher, forwarded_by: Some(me.to_string()), changed_at_rafka_ms })
        }
        Frame::FabricStatus { fabric, status, publisher, forwarded_by: None, changed_at_rafka_ms } if publisher != me => {
            Some(Frame::FabricStatus { fabric, status, publisher, forwarded_by: Some(me.to_string()), changed_at_rafka_ms })
        }
        _ => None,
    }
}

pub(crate) fn reserved_name(position: usize) -> &'static str {
    match position { 7 => "node-restarting", 10 => "concern", 11 => "node-connections-deleting", 12 => "node-leaving", 13 => "node-left", 14 => "node-connections-deleted", 15 => "new-fabric-primary", _ => unreachable!() }
}

/// Split `items` into runs whose frame (as `frame` builds it, sized as a forward would be)
/// encodes within [`MAX_FRAME`], by the one method the Build facts share
/// ([`rdm_transport::chunking::pack_in_order`]). An item that fits no message on its own is left out and
/// named.
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
pub(crate) fn pack<T: Clone>(items: Vec<T>, frame: impl Fn(Vec<T>) -> Frame) -> Vec<Vec<T>> {
    let (runs, refused) = rdm_transport::chunking::pack_in_order(items, |run: &[T]| {
        let bytes = frame(run.to_vec()).encode();
        if bytes.len() > MAX_FRAME {
            Err(bytes.len())
        } else {
            Ok(Bytes::from(bytes))
        }
    });
    for len in refused {
        tracing::info_span!("rdm.mesh.topology.reject.via-item-too-large", frame_bytes = len, limit = MAX_FRAME)
            .in_scope(|| tracing::warn!("a member, overlay or departure that fits no gossip message on its own: left out"));
    }
    runs.into_iter().map(|(run, _)| run).collect()
}
