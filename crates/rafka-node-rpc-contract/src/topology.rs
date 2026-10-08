//! The topology read on op `0x1E` (node-rpc-envelope.md "Topology, op `0x1E`, rdm").
//!
//! Direction: peer read, any node to any node. The call is read-only: it mutates nothing at the
//! target. The reply is a server stream of the snapshots the target holds, one per mesh, chunked
//! by the one method gossip's `Members` snapshots use:
//!
//! ```text
//! Started  Snapshot* | Unchanged*  End { meshes }       a stream
//! NotReady | UnknownMesh                                  one refusal, nothing started
//! ```
//!
//! A mesh is named by its NAME on the wire, as the gossip `Members` frame names it. A caller
//! installs a mesh atomically, and only once every chunk of one `(snapshot_id, publisher,
//! topology_version)` is present. The family is not forwardable: it is a stream.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use rafka_mesh_entity::wire::WireDigest;
use rafka_mesh_entity::{LifecycleOp, PublisherId};
use serde::{Deserialize, Serialize};

pub struct Topology;

/// The source version a caller already holds for a mesh: the publisher is the epoch, so
/// `topology_version` is compared only inside one publisher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceVersion {
    pub publisher: PublisherId,
    pub topology_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyRequest {
    /// `mesh = None` asks for every mesh the target holds. A held `since` for a mesh answers
    /// `Unchanged` for it.
    GetTopology { mesh: Option<String>, since: Option<SourceVersion> },
}

impl TopologyRequest {
    pub fn op(&self) -> &'static str {
        match self {
            Self::GetTopology { .. } => "get-topology",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyReply {
    /// One chunk of one mesh's snapshot.
    Snapshot {
        mesh: String,
        publisher: PublisherId,
        topology_version: u64,
        snapshot_id: u64,
        chunk_index: u32,
        chunk_count: u32,
        digests: Vec<WireDigest>,
        in_flight: Vec<LifecycleOp>,
        departed: Vec<LifecycleOp>,
    },
    /// The caller's `since` is the target's held source version for `mesh`.
    Unchanged { mesh: String, publisher: PublisherId, topology_version: u64 },
    /// The stream is complete; `meshes` counts the meshes answered, `Unchanged` included.
    End { meshes: u32 },
    /// The target holds no topology yet.
    NotReady { reason: String },
    /// The requested mesh is not one the target holds.
    UnknownMesh { mesh: String },
    PeerUnresolved { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    /// The stream begins: sent before the first snapshot of an accepted read.
    Started,
}

impl TopologyReply {
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
    const REPLY_VARIANTS: u32 = 11;

    type Request = TopologyRequest;
    type Reply = TopologyReply;

    fn classify_reply(reply: &TopologyReply) -> ReplyKind {
        match reply {
            TopologyReply::Snapshot { .. } | TopologyReply::Unchanged { .. } | TopologyReply::End { .. } | TopologyReply::Started => ReplyKind::Success,
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
            TopologyReply::Snapshot { .. } | TopologyReply::Unchanged { .. } => FrameKind::Data,
            TopologyReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Topology::classify_reply(other)),
        }
    }
}
