//! The gossip frames (gossip.md §3.1): the `Frame` enum, its wire positions and the forwarding
//! a receiving primary applies to a frame.


use crate::snapshot::PublisherId;
use anyhow::Result;
use rdm_core::{FabricId, IncarnationId, LifecycleOp, MeshDigest, NodeId, Primary, PrimaryRole, StateHook};
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
/// [`Frame::Authored`] is never serialized. MeshTopology publication identity in data is
/// `(fabric_id, node_id, incarnation, counter)`; the frame retains the counter alongside its authored
/// context. A field with a serde `with` module travels in that module's wire shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    /// A member's own digest (its mesh channel only).
    ///
    /// Schema index 0, never a wire kind.
    Digest {
        /// The member's digest, in its wire shape.
        #[serde(with = "rdm_transport::wire::digest")]
        digest: MeshDigest,
    },
    /// One chunk of a snapshot of `mesh`'s topology (gossip.md §3.1). On the backbone it is the
    /// complete Mesh with its loads, published by that Mesh's current primary (`publisher`, the
    /// exact birth that is its primary, whose identity is the revision of `topology_version`); on a peer
    /// Mesh's own channel it is the forwarded full without loads, `publisher` still the SOURCE
    /// primary and `forwarded_by` the local primary that sends it (the backbone frame carries
    /// `None`). `topology_version` bumps on a material change of the normalized projection and
    /// never on a heartbeat, a stamp, a load or an unchanged republish. All chunks of one
    /// snapshot share `snapshot_id` (the sender's own counter, never a clock reading) and carry
    /// the open lifecycle overlays (`in_flight`) and retained deletions (`deleted`)
    /// split across them; a receiver installs the snapshot only once it holds every chunk.
    /// `published_at_rafka_ms` makes each publication distinct.
    ///
    /// Schema index 1, never a wire kind.
    MeshTopology {
        /// The source mesh's name.
        mesh: String,
        /// The source mesh's primary birth that published the snapshot, the revision of
        /// `topology_version`.
        publisher: PublisherId,
        /// The local primary that forwards the snapshot onto its own channel; `None` on the
        /// backbone.
        forwarded_by: Option<String>,
        /// The topology version the snapshot carries.
        topology_version: u64,
        /// The Rafka-time of the publication, making each publication distinct.
        published_at_rafka_ms: u64,
        /// The publication owner's counter, shared by every chunk. The wire data pairs it with that
        /// owner's exact fabric/node/incarnation birth from the authored context.
        snapshot_id: u64,
        /// This chunk's index within the snapshot.
        chunk_index: u32,
        /// The number of chunks in the snapshot.
        chunk_count: u32,
        /// The members this chunk carries, each a member projection entry: its digest, then the
        /// status its mesh-primary holds (gossip.md §3.1).
        #[serde(with = "rdm_transport::wire::entries")]
        digests: Vec<MeshDigest>,
        /// The open lifecycle overlays this chunk carries.
        in_flight: Vec<LifecycleOp>,
        /// The retained deletions this chunk carries.
        deleted: Vec<LifecycleOp>,
    },
    /// What moved a source Mesh's held projection from `base_version` to `topology_version`,
    /// published on the forwarding primary's own mesh channel (gossip.md §3.1, §3.3). `base_version`
    /// is the version that primary last published into its Mesh; a receiver applies the delta only
    /// when it holds exactly that version of `source_publisher`'s Mesh. Loads are omitted.
    /// `in_flight` and `deleted` are the overlays and deletions added since `base_version`.
    ///
    /// Schema index 2, never a wire kind.
    MeshTopologyDelta {
        /// The source mesh's name.
        mesh: String,
        /// The source mesh's primary birth the delta is derived from.
        source_publisher: PublisherId,
        /// The version a receiver must hold for the delta to apply.
        base_version: u64,
        /// The version the delta moves to.
        topology_version: u64,
        /// The Rafka-time of the publication.
        published_at_rafka_ms: u64,
        /// The births added or changed, each a member projection entry: its digest, then the status
        /// its mesh-primary holds (gossip.md §3.1).
        #[serde(with = "rdm_transport::wire::entries")]
        changed: Vec<MeshDigest>,
        /// The node ids no longer listed.
        removed: Vec<String>,
        /// The overlays added since the base version.
        in_flight: Vec<LifecycleOp>,
        /// The deletions added since the base version.
        deleted: Vec<LifecycleOp>,
    },
    /// The mesh executor holding `op` has started removing its exact birth:
    /// the node is still found, and application routing stops selecting it.
    /// Published on the executor's own mesh channel only; the backbone carries it as an open
    /// overlay of its mesh's topology (`MeshTopology.in_flight`).
    ///
    /// Schema index 3, never a wire kind.
    NodeDeleting {
        /// The removal being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The exact birth that published the fact: the revision of `topology_version`. A receiver
        /// orders the facts of one publisher and mesh by that version (R-RX3), whoever published them.
        publisher: PublisherId,
        /// The publisher's `topology_version` of the subject's mesh at which the subject changed.
        topology_version: u64,
    },
    /// The provider proved `op`'s exact birth terminal: it has left. Same
    /// channel as `NodeDeleting`; retained afterwards in `MeshTopology.deleted`.
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
    /// channel: a primary change rides the successor's `Digest` and the mesh-primary's `MeshTopology`
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
        /// The primary's control API base URL with its bound HTTP port, when it runs one.
        admin_api_base: Option<String>,
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
    /// channel as `NodeDeleting`; carried in `MeshTopology.in_flight` while open.
    ///
    /// Schema index 18, never a wire kind.
    NodeStopped {
        /// The stop being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The exact birth that published the fact: the revision of `topology_version`, as on
        /// [`Frame::NodeDeleting`].
        publisher: PublisherId,
        /// The publisher's `topology_version` of the subject's mesh at which the subject changed: a
        /// projection of that publisher newer than it, listing the birth, moved past the stop.
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
    /// The selected domain data, independent of its immutable authored context.
    pub fn data(&self) -> &Self { match self { Self::Authored { frame, .. } => frame.data(), _ => self } }
    /// Consume the carrier and retain its selected data.
    pub fn into_data(self) -> Self { match self { Self::Authored { frame, .. } => frame.into_data(), _ => self } }
    /// The original authored context, when this frame is ready for the wire.
    pub fn context(&self) -> Option<&crate::envelope::Context> { match self { Self::Authored { context, .. } => Some(context), _ => None } }
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
        Frame::NodeDeleting { op, publisher, topology_version, .. } if op.mesh != own_mesh => Some(Frame::NodeDeleting { op, forwarded_by: Some(me.to_string()), publisher, topology_version }),
        Frame::NodeDeleted { op, .. } if op.mesh != own_mesh => Some(Frame::NodeDeleted { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeStopped { op, publisher, topology_version, .. } if op.mesh != own_mesh => Some(Frame::NodeStopped { op, forwarded_by: Some(me.to_string()), publisher, topology_version }),
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
