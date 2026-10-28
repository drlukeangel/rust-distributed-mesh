//! The topology read on op `0x1E` (node-rpc-envelope.md "Topology, op `0x1E`, rdm").
//!
//! Direction: peer read, any node to any node. The call is read-only: it mutates nothing at the
//! target. The reply is a server stream of the snapshots the target holds, one per mesh, chunked
//! by the one method gossip's `MeshTopology` snapshots use:
//!
//! ```text
//! Started  (Snapshot* | Unchanged* | Stored*) Primaries* RafkaTime  End { meshes }   a stream
//! NotReady | UnknownMesh                                  one refusal, nothing started
//! ```
//!
//! A mesh the target holds no gossiped snapshot of, but whose nodes it stores, is answered with
//! `Stored`: the stored map, which carries no version. It names births to reach and is never the
//! mesh's current topology: a caller installs none of it.
//!
//! A mesh is named by its NAME on the wire, as the gossip `MeshTopology` frame names it. A caller
//! installs a mesh atomically, and only once every chunk of one `(snapshot_id, publisher,
//! topology_version)` is present. The family is not forwardable: it is a stream.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::wire::ProjectionEntry;
use crate::{EndpointId, IncarnationId, LifecycleOp, MeshId, NodeId, PublisherId, Primary};
use serde::{Deserialize, Serialize};

/// A primary as the topology answers it (node-rpc-envelope.md `PrimaryKind`): the election's result
/// the mesh-primary recorded as `is_primary` (a mesh-primary) or `is_fabric_primary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrimaryKind {
    /// A mesh's mesh-primary.
    MeshPrimary,
    /// The fabric-primary.
    FabricPrimary,
}

impl PrimaryKind {
    /// The kind's name as spans name it.
    pub fn name(self) -> &'static str {
        match self {
            Self::MeshPrimary => "mesh-primary",
            Self::FabricPrimary => "fabric-primary",
        }
    }
}

/// The topology read protocol: a stream of one mesh's snapshot chunks, or of every mesh a node
/// holds.
pub struct Topology;

/// One node of a stored map: how to reach a birth the target stored, with no claim that it lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredNode {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: String,
    /// The node's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The incarnation the target stored.
    pub incarnation: IncarnationId,
    /// The address the target stored for the node's endpoint.
    pub transport_addr: std::net::SocketAddr,
}

/// The source version a caller already holds for a mesh: the publisher is the revision, so
/// `topology_version` is compared only inside one publisher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceVersion {
    /// The source mesh's primary birth that published the version, the revision of `topology_version`.
    pub publisher: PublisherId,
    /// The version held.
    pub topology_version: u64,
}

/// A topology read call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyRequest {
    /// `mesh = None` asks for every mesh the target holds. A held `since` for a mesh answers
    /// `Unchanged` for it.
    GetTopology {
        /// The mesh to read; `None` for every mesh the target holds.
        mesh: Option<String>,
        /// The source version the caller already holds, when any.
        since: Option<SourceVersion>,
    },
}

impl TopologyRequest {
    /// The request's operation name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::GetTopology { .. } => "get-topology",
        }
    }
}

/// One frame of a topology read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyReply {
    /// One chunk of one mesh's snapshot.
    Snapshot {
        /// The source mesh's name.
        mesh: String,
        /// The publisher of the snapshot.
        publisher: PublisherId,
        /// The snapshot's `topology_version`.
        topology_version: u64,
        /// The publisher's own snapshot counter, shared by every chunk of the snapshot.
        snapshot_id: u64,
        /// This chunk's index within the snapshot.
        chunk_index: u32,
        /// The number of chunks in the snapshot.
        chunk_count: u32,
        /// The members this chunk carries, each a member projection entry: its digest, then the
        /// status held for it (gossip.md §3.1).
        digests: Vec<ProjectionEntry>,
        /// The open lifecycle overlays this chunk carries.
        in_flight: Vec<LifecycleOp>,
        /// The retained proven departures this chunk carries.
        departed: Vec<LifecycleOp>,
    },
    /// The caller's `since` is the target's held source version for `mesh`.
    Unchanged {
        /// The source mesh's name.
        mesh: String,
        /// The publisher of the version held.
        publisher: PublisherId,
        /// The version held.
        topology_version: u64,
    },
    /// The stream is complete; `meshes` counts the meshes answered, `Unchanged` included.
    End {
        /// The number of meshes answered.
        meshes: u32,
    },
    /// The target holds no topology yet.
    NotReady {
        /// Why the target holds no topology.
        reason: String,
    },
    /// The requested mesh is not one the target holds.
    UnknownMesh {
        /// The mesh requested.
        mesh: String,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The node is at its admission bound.
    Busy {
        /// Which bound the node is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ConnectionsDeleting {
        /// Why the node refuses new work.
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
    /// The stream begins: sent before the first snapshot of an accepted read.
    Started,
    /// A mesh the target holds no gossiped snapshot of, from the map it stores: no version, so it is
    /// never installed as the mesh's topology. It names births to reach, nothing more. One mesh may
    /// arrive in several frames; `End.meshes` counts the mesh once. `mesh_id` is the id the target
    /// stored for the mesh, when it stored one: the mesh keeps its id through a recovery.
    Stored {
        /// The mesh's name.
        mesh: String,
        /// The id the target stored for the mesh, when it stored one.
        mesh_id: Option<MeshId>,
        /// The births the target stored for the mesh.
        nodes: Vec<StoredNode>,
    },
    /// A primary the target holds as the election's result the mesh-primary recorded (`is_primary`,
    /// `is_fabric_primary`): a node entering the fabric learns the recorded primaries, an election
    /// input, before it computes any (elections §3). One frame per record, sent before `End`; it is
    /// not a mesh and `End.meshes` does not count it. `gone` says the target holds the primary's
    /// exact birth as gone (a later birth replaced it, or its departure was heard).
    Primaries {
        /// Which primary the record is.
        kind: PrimaryKind,
        /// The primary as the target records it.
        primary: Primary,
        /// Whether the target holds the primary's exact birth as gone.
        gone: bool,
    },
    /// The rafka-time the target holds, and which primary it is: one frame, sent after the
    /// `Primaries` records and before `End`. A puller adopts the time by its own role's rule (a member
    /// adopts from whoever answers; a mesh-primary only from the fabric-primary). The target answers
    /// the rafka-time it adopted and never its own clock; a target that holds none answers `NotReady`
    /// and starts no stream.
    RafkaTime {
        /// The target's rafka-time now, in milliseconds.
        ms: u64,
        /// Which primary the target is, from the primaries it holds: the fabric-primary when it is,
        /// else its mesh's mesh-primary, else none.
        kind: Option<PrimaryKind>,
    },
    /// The reader's own whole row as its mesh-primary's atlas holds it (Q234: every node property:
    /// config, tags, executable, storage, placement, listeners, status, primaries), never read from the
    /// store: one frame, answered only by the reader's own mesh-primary to a node of its mesh, after
    /// `RafkaTime` and before `End`; it is not a mesh and `End.meshes` does not count it. The reader
    /// holds it on its own entry in its topology.
    OwnRow {
        /// The reader's row.
        node: crate::views::NodeView,
        /// The row's revision (P1): the reader installs the row over an older revision.
        revision: u64,
    },
}

impl TopologyReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::Unchanged { .. } => "unchanged",
            Self::End { .. } => "end",
            Self::NotReady { .. } => "not-ready",
            Self::UnknownMesh { .. } => "unknown-mesh",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Started => "started",
            Self::Stored { .. } => "stored",
            Self::Primaries { .. } => "primaries",
            Self::RafkaTime { .. } => "rafka-time",
            Self::OwnRow { .. } => "own-row",
        }
    }
}

impl NodeProtocol for Topology {
    const OP: u8 = 0x1E;
    const NAME: &'static str = "topology";
    /// A mesh name and one source version.
    const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024;
    /// One chunk: gossip's message bound (`rafka-mesh-transport` chunking) plus the frame's own fields.
    const MAX_REPLY_FRAME_BYTES: usize = 8 * 1024;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 15;

    type Request = TopologyRequest;
    type Reply = TopologyReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            TopologyReply::Snapshot { .. } => "snapshot",
            TopologyReply::Unchanged { .. } => "unchanged",
            TopologyReply::End { .. } => "end",
            TopologyReply::NotReady { .. } => "not-ready",
            TopologyReply::UnknownMesh { .. } => "unknown-mesh",
            TopologyReply::PeerUnresolved { .. } => "peer-unresolved",
            TopologyReply::Busy { .. } => "busy",
            TopologyReply::ConnectionsDeleting { .. } => "connections-deleting",
            TopologyReply::Malformed { .. } => "malformed",
            TopologyReply::Unauthorized { .. } => "unauthorized",
            TopologyReply::Started => "started",
            TopologyReply::Stored { .. } => "stored",
            TopologyReply::Primaries { .. } => "primaries",
            TopologyReply::RafkaTime { .. } => "rafka-time",
            TopologyReply::OwnRow { .. } => "own-row",
        }
    }

    fn classify_reply(reply: &TopologyReply) -> ReplyKind {
        match reply {
            TopologyReply::Snapshot { .. } => ReplyKind::Success,
            TopologyReply::Unchanged { .. } => ReplyKind::Success,
            TopologyReply::End { .. } => ReplyKind::Success,
            TopologyReply::NotReady { .. } => ReplyKind::NotReady,
            TopologyReply::UnknownMesh { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            TopologyReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            TopologyReply::Busy { .. } => ReplyKind::Busy,
            TopologyReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            TopologyReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            TopologyReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            TopologyReply::Started  => ReplyKind::Success,
            TopologyReply::Stored { .. } => ReplyKind::Success,
            TopologyReply::Primaries { .. } => ReplyKind::Success,
            TopologyReply::RafkaTime { .. } => ReplyKind::Success,
            TopologyReply::OwnRow { .. } => ReplyKind::Success,
        }
    }

    fn peer_unresolved(reason: String) -> TopologyReply {
        TopologyReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> TopologyReply {
        TopologyReply::NotReady { reason }
    }
    fn busy(reason: String) -> TopologyReply {
        TopologyReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> TopologyReply {
        TopologyReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> TopologyReply {
        TopologyReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> TopologyReply {
        TopologyReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for Topology {
    fn frame_kind(frame: &TopologyReply) -> FrameKind {
        match frame {
            TopologyReply::Started => FrameKind::Started,
            TopologyReply::Snapshot { .. } | TopologyReply::Unchanged { .. } | TopologyReply::Stored { .. } | TopologyReply::Primaries { .. } | TopologyReply::RafkaTime { .. } | TopologyReply::OwnRow { .. } => FrameKind::Data,
            TopologyReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Topology::classify_reply(other)),
        }
    }
}
