//! The topology read on op `0x1E` (node-rpc-envelope.md "Topology, op `0x1E`, rdm").
//!
//! Direction: peer read, any node to any node. The call is read-only: it mutates nothing at the
//! target. The reply is a server stream of the snapshots the target holds, one per mesh, chunked
//! by the one method gossip's `Members` snapshots use:
//!
//! ```text
//! Started  (Snapshot* | Unchanged* | Stored*) Seats*  End { meshes }   a stream
//! NotReady | UnknownMesh                                  one refusal, nothing started
//! ```
//!
//! A mesh the target holds no gossiped snapshot of, but whose nodes it stores, is answered with
//! `Stored`: the stored map, which carries no version. It names births to reach and is never the
//! mesh's current topology: a caller installs none of it.
//!
//! A mesh is named by its NAME on the wire, as the gossip `Members` frame names it. A caller
//! installs a mesh atomically, and only once every chunk of one `(snapshot_id, publisher,
//! topology_version)` is present. The family is not forwardable: it is a stream.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use rafka_mesh_entity::wire::WireDigest;
use rafka_mesh_entity::{EndpointId, IncarnationId, LifecycleOp, MeshId, NodeId, PublisherId, Seat, SeatHolder};
use serde::{Deserialize, Serialize};

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

/// The source version a caller already holds for a mesh: the publisher is the epoch, so
/// `topology_version` is compared only inside one publisher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceVersion {
    /// The source mesh's primary birth that published the version, the epoch of `topology_version`.
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
        /// The members this chunk carries.
        digests: Vec<WireDigest>,
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
    /// The node is draining and takes no new work.
    Draining {
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
    /// The record the target holds for one seat: a node entering the fabric learns who holds the
    /// seats before it computes any (ruling R-A2). One frame per record, sent before `End`; it is
    /// not a mesh and `End.meshes` does not count it.
    Seats {
        /// The seat the record is for.
        seat: Seat,
        /// The seat's holder as the target records it.
        holder: SeatHolder,
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
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Started => "started",
            Self::Stored { .. } => "stored",
            Self::Seats { .. } => "seats",
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
    const REPLY_VARIANTS: u32 = 13;

    type Request = TopologyRequest;
    type Reply = TopologyReply;

    fn classify_reply(reply: &TopologyReply) -> ReplyKind {
        match reply {
            TopologyReply::Snapshot { .. } | TopologyReply::Unchanged { .. } | TopologyReply::Stored { .. } | TopologyReply::Seats { .. } | TopologyReply::End { .. } | TopologyReply::Started => ReplyKind::Success,
            TopologyReply::UnknownMesh { .. } => ReplyKind::ProtocolRefusal,
            TopologyReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            TopologyReply::NotReady { .. } => ReplyKind::NotReady,
            TopologyReply::Busy { .. } => ReplyKind::Busy,
            TopologyReply::Draining { .. } => ReplyKind::Draining,
            TopologyReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            TopologyReply::Unauthorized { .. } => ReplyKind::Unauthorized,
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
    fn draining(reason: String) -> TopologyReply {
        TopologyReply::Draining { reason }
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
            TopologyReply::Snapshot { .. } | TopologyReply::Unchanged { .. } | TopologyReply::Stored { .. } | TopologyReply::Seats { .. } => FrameKind::Data,
            TopologyReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Topology::classify_reply(other)),
        }
    }
}
