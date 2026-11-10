//! The reads on op `0x23` (node-rpc-envelope.md "Read, op `0x23`, rdm"; Q285).
//!
//! Direction: a caller to a node. The call is read-only: it changes nothing at the target. The
//! reply is a server stream of the rows the target answers for one read, `fabric.get`, `mesh.get`,
//! `node.get`, `node.connection.get { node }`, `fabric.atlas.get` or `mesh.atlas.get`, captured once
//! in their stable order and packed into frames:
//!
//! ```text
//! Started  (Nodes | Meshes | Fabrics | Connections | Atlas)*  End { rows, frames }   a stream
//! RejectedFilter | RejectedSourceFailed | NotReady | ReadOverBound | ...             one refusal, nothing started
//! ```
//!
//! A row that fits no frame alone is `ReadOverBound` naming it, and nothing of the read is sent. A
//! caller counts the read only when `End` arrives and the rows it holds number `End.rows` across
//! `End.frames` data frames; a cut, a deadline or a `ReadOverBound` is a failed read, and no row is
//! ever left out. Status (0x1B) stays unary: its `Get` of any entity, and the retained `GetNodes`,
//! `GetMeshes`, `GetConnections`, `GetAtlas` and `GetMeshAtlas`, are refused by name, naming this op;
//! the retained `GetFabric` stays on Status. `GetConnections` is answered by the exact birth it names,
//! from its own connections log, never carried; `GetAtlas` and `GetMeshAtlas` by the node-admin asked,
//! over the meshes it is primary for, never carried.
//!
//! The family is not forwardable: it is a stream.

use crate::filter::{Entity, Filter};
use crate::ids::{IncarnationId, NodeId};
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::views::{AtlasFabricView, AtlasMeshView, AtlasNodeView, AtlasView, ConnectionView, ConnectionsView, FabricView, MeshView, NodeView};
use serde::{Deserialize, Serialize};

/// The read protocol: a stream of the rows one read answers.
pub struct Read;

/// A read call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadRequest {
    /// Every row of `entity` the target answers that matches every pair of `filter`, in a stable
    /// order (Q167): `fabric.get`, `mesh.get` or `node.get`. `Connections` is refused here by name,
    /// naming [`Self::GetConnections`].
    ///
    /// Position 0 on the wire.
    Get {
        /// The entity read.
        entity: Entity,
        /// The pairs every answered row matches.
        filter: Filter,
    },
    /// `node.connection.get { node }`: the connections the exact birth `(node, incarnation)` holds as
    /// its own that match every pair of `filter`, in a stable order (Q167 R-RD7, Q285); the birth is
    /// the read's address.
    ///
    /// Position 1 on the wire.
    GetConnections {
        /// The node read.
        node: NodeId,
        /// The exact birth of that node.
        incarnation: IncarnationId,
        /// The pairs every answered connection matches.
        filter: Filter,
    },
    /// `fabric.atlas.get` (query.md §1, Q439): the answering node-admin's atlas over every mesh it is
    /// primary for, streamed in [`ReadReply::Atlas`] frames. Appended last: no variant before it moves.
    ///
    /// Position 2 on the wire.
    GetAtlas,
    /// `mesh.atlas.get` (query.md §1, Q439): the same, with only the meshes `filter` names (the mesh
    /// schema's pairs, `[name: …]`); a mesh it does not hold is its fabric with no mesh, and a pair the
    /// mesh schema refuses is [`ReadReply::RejectedFilter`] naming it. Appended last: no variant before
    /// it moves.
    ///
    /// Position 3 on the wire.
    GetMeshAtlas {
        /// The meshes asked for.
        filter: Filter,
    },
}

impl ReadRequest {
    /// The request's operation name as it appears in spans and replies: the read's own name.
    pub fn op(&self) -> &'static str {
        match self {
            Self::Get { entity, .. } => entity.read(),
            Self::GetConnections { .. } => "node.connection.get",
            Self::GetAtlas => "fabric.atlas.get",
            Self::GetMeshAtlas { .. } => "mesh.atlas.get",
        }
    }
}

/// One frame of a read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadReply {
    /// The stream begins, naming the read.
    ///
    /// Position 0 on the wire.
    Started {
        /// The read (`node.get`, `mesh.get`, `fabric.get`).
        read: String,
    },
    /// A batch of `node.get`'s rows, in the read's order.
    ///
    /// Position 1 on the wire.
    Nodes {
        /// The rows.
        nodes: Vec<NodeView>,
    },
    /// A batch of `mesh.get`'s rows, in the read's order.
    ///
    /// Position 2 on the wire.
    Meshes {
        /// The rows.
        meshes: Vec<MeshView>,
    },
    /// A batch of `fabric.get`'s rows, in the read's order.
    ///
    /// Position 3 on the wire.
    Fabrics {
        /// The rows.
        fabrics: Vec<FabricView>,
    },
    /// The stream is over: `rows` rows were sent in `frames` data frames.
    ///
    /// Position 4 on the wire.
    End {
        /// The number of rows sent.
        rows: u32,
        /// The number of data frames sent.
        frames: u32,
    },
    /// A filter the entity's schema refuses, or an entity this op does not read, named.
    ///
    /// Position 5 on the wire.
    RejectedFilter {
        /// Why, naming the pair.
        reason: String,
    },
    /// A row that fits no frame: nothing of it is sent, and the stream ends here with no `End`.
    ///
    /// Position 6 on the wire.
    ReadOverBound {
        /// The read.
        read: String,
        /// The row, by its name or id.
        row: String,
        /// The row's frame size.
        bytes: u64,
        /// The frame bound.
        max: u64,
    },
    /// A source the answer is read from failed, named.
    ///
    /// Position 7 on the wire.
    RejectedSourceFailed {
        /// What failed.
        source: String,
        /// The operation on it.
        op: String,
        /// Why.
        cause: String,
    },
    /// The target is not ready to serve; `reason` names what it waits for.
    ///
    /// Position 8 on the wire.
    NotReady {
        /// What the target waits for.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 9 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The target is at its admission bound.
    ///
    /// Position 10 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The target is connections-deleting and takes no new work.
    ///
    /// Position 11 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 12 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 13 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// A batch of `node.connection.get`'s rows, in the read's order: every frame of one read carries
    /// the same `node` (the answering node's held `(path, name)`) and `complete` (whether it holds the
    /// complete set).
    ///
    /// Position 14 on the wire.
    Connections {
        /// The answering node's held `(path, name)`.
        node: String,
        /// Whether the node holds the complete set.
        complete: bool,
        /// The rows.
        connections: Vec<ConnectionView>,
    },
    /// A batch of `fabric.atlas.get`'s or `mesh.atlas.get`'s rows (query.md §1, Q439): `fabric` with
    /// its meshes empty, `mesh` with its nodes empty, and that mesh's node rows in order. A fabric with
    /// no mesh is one frame with no mesh, a mesh with no node one frame with no node. Appended last: no
    /// variant before it moves.
    ///
    /// Position 15 on the wire.
    Atlas {
        /// The fabric, its meshes empty.
        fabric: AtlasFabricView,
        /// The mesh, its nodes empty; `None` for a fabric with no mesh.
        mesh: Option<AtlasMeshView>,
        /// The mesh's node rows, in `(path, name)` order.
        nodes: Vec<AtlasNodeView>,
    },
}

impl ReadReply {
    /// The reply's name as spans and refusals name it.
    pub fn name(&self) -> &'static str {
        <Read as NodeProtocol>::reply_detail(self)
    }

    /// The rows a data frame carries; `0` for any other frame. An atlas frame's rows are its node
    /// rows, or the one fabric or mesh it carries with none.
    pub fn rows(&self) -> usize {
        match self {
            Self::Nodes { nodes } => nodes.len(),
            Self::Meshes { meshes } => meshes.len(),
            Self::Fabrics { fabrics } => fabrics.len(),
            Self::Connections { connections, .. } => connections.len(),
            Self::Atlas { nodes, .. } => nodes.len().max(1),
            _ => 0,
        }
    }
}

impl NodeProtocol for Read {
    const OP: u8 = 0x23;
    const NAME: &'static str = "read";
    /// An entity and its filter pairs.
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    /// One data frame: a batch of rows packed to it in order; a row larger alone is `ReadOverBound`.
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const FORWARDABLE: bool = false;
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 4;
    const REPLY_VARIANTS: u32 = 16;

    type Request = ReadRequest;
    type Reply = ReadReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            ReadReply::Started { .. } => "started",
            ReadReply::Nodes { .. } => "nodes",
            ReadReply::Meshes { .. } => "meshes",
            ReadReply::Fabrics { .. } => "fabrics",
            ReadReply::Connections { .. } => "connections",
            ReadReply::Atlas { .. } => "atlas",
            ReadReply::End { .. } => "end",
            ReadReply::RejectedFilter { .. } => "rejected-filter",
            ReadReply::ReadOverBound { .. } => "read-over-bound",
            ReadReply::RejectedSourceFailed { .. } => "rejected-source-failed",
            ReadReply::NotReady { .. } => "not-ready",
            ReadReply::PeerUnresolved { .. } => "peer-unresolved",
            ReadReply::Busy { .. } => "busy",
            ReadReply::ConnectionsDeleting { .. } => "connections-deleting",
            ReadReply::Malformed { .. } => "malformed",
            ReadReply::Unauthorized { .. } => "unauthorized",
        }
    }

    fn classify_reply(reply: &ReadReply) -> ReplyKind {
        match reply {
            ReadReply::Started { .. } | ReadReply::Nodes { .. } | ReadReply::Meshes { .. } | ReadReply::Fabrics { .. } | ReadReply::Connections { .. } | ReadReply::Atlas { .. } | ReadReply::End { .. } => ReplyKind::Success,
            ReadReply::RejectedFilter { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            ReadReply::ReadOverBound { .. } => ReplyKind::new(crate::outcome::Code::ResourceExhausted),
            ReadReply::RejectedSourceFailed { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            ReadReply::NotReady { .. } => ReplyKind::NotReady,
            ReadReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ReadReply::Busy { .. } => ReplyKind::Busy,
            ReadReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            ReadReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ReadReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> ReadReply {
        ReadReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ReadReply {
        ReadReply::NotReady { reason }
    }
    fn busy(reason: String) -> ReadReply {
        ReadReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> ReadReply {
        ReadReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> ReadReply {
        ReadReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ReadReply {
        ReadReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for Read {
    fn frame_kind(frame: &ReadReply) -> FrameKind {
        match frame {
            ReadReply::Started { .. } => FrameKind::Started,
            ReadReply::Nodes { .. } | ReadReply::Meshes { .. } | ReadReply::Fabrics { .. } | ReadReply::Connections { .. } | ReadReply::Atlas { .. } => FrameKind::Data,
            ReadReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Read::classify_reply(other)),
        }
    }
}

/// The rows one read answers, in its stable order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rows {
    /// `node.get`'s.
    Nodes(Vec<NodeView>),
    /// `mesh.get`'s.
    Meshes(Vec<MeshView>),
    /// `fabric.get`'s.
    Fabrics(Vec<FabricView>),
    /// `node.connection.get`'s.
    Connections(ConnectionsView),
    /// `fabric.atlas.get`'s or `mesh.atlas.get`'s: the Q439 atlas, its frames assembled whole.
    Atlas(AtlasView),
}

impl Rows {
    /// The number of rows. An atlas row is one node row, or a fabric or mesh with none.
    pub fn len(&self) -> usize {
        match self {
            Self::Nodes(r) => r.len(),
            Self::Meshes(r) => r.len(),
            Self::Fabrics(r) => r.len(),
            Self::Connections(r) => r.connections.len(),
            Self::Atlas(a) => a.fabrics.iter().map(|f| if f.meshes.is_empty() { 1 } else { f.meshes.iter().map(|m| m.nodes.len().max(1)).sum() }).sum(),
        }
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The rows with `node_id`'s own load and mesh-channel counts on its own row: what the node that
    /// answers a `node.get` serves of itself, whoever's atlas the rows came from (Q394;
    /// node-identity-and-state.md §2.5: in-memory diagnostics of the node itself, served only for the
    /// node that answers). Rows of another entity, and every other node's row, are as they were.
    pub fn with_own_sample(mut self, node_id: &crate::NodeId, load: crate::NodeLoad, gossip: crate::GossipStats) -> Self {
        if let Self::Nodes(nodes) = &mut self {
            crate::views::own_sample_on(nodes, node_id, load, gossip);
        }
        self
    }

    /// The empty rows of `entity`; `None` for an entity this request does not read.
    pub fn empty(entity: Entity) -> Option<Self> {
        match entity {
            Entity::Nodes => Some(Self::Nodes(Vec::new())),
            Entity::Meshes => Some(Self::Meshes(Vec::new())),
            Entity::Fabrics => Some(Self::Fabrics(Vec::new())),
            Entity::Connections => None,
        }
    }

    /// Append a data frame's rows; `false` for a frame of another entity's rows, a `Connections`
    /// frame that disagrees with an earlier frame's `node` or `complete`, or an `Atlas` frame with
    /// node rows and no mesh. An `Atlas` frame of the fabric before it adds its mesh, and one of the mesh
    /// before it adds its nodes.
    pub fn take(&mut self, frame: ReadReply) -> bool {
        match (self, frame) {
            (Self::Nodes(r), ReadReply::Nodes { nodes }) => r.extend(nodes),
            (Self::Meshes(r), ReadReply::Meshes { meshes }) => r.extend(meshes),
            (Self::Fabrics(r), ReadReply::Fabrics { fabrics }) => r.extend(fabrics),
            (Self::Connections(r), ReadReply::Connections { node, complete, connections }) => {
                if node.is_empty() || (!r.node.is_empty() && (r.node != node || r.complete != complete)) {
                    return false;
                }
                r.node = node;
                r.complete = complete;
                r.connections.extend(connections);
            }
            (Self::Atlas(a), ReadReply::Atlas { fabric, mesh, nodes }) => {
                if mesh.is_none() && !nodes.is_empty() {
                    return false;
                }
                let fabric = match a.fabrics.last_mut() {
                    Some(held) if held.id == fabric.id => held,
                    _ => {
                        a.fabrics.push(fabric);
                        a.fabrics.last_mut().expect("the fabric just added")
                    }
                };
                if let Some(mesh) = mesh {
                    let mesh = match fabric.meshes.last_mut() {
                        Some(held) if held.name == mesh.name => held,
                        _ => {
                            fabric.meshes.push(mesh);
                            fabric.meshes.last_mut().expect("the mesh just added")
                        }
                    };
                    mesh.nodes.extend(nodes);
                }
            }
            _ => return false,
        }
        true
    }

    /// The rows one at a time, in order, for the packer. An atlas is one row per node row, each with
    /// its fabric (meshes empty) and its mesh (nodes empty); a fabric with no mesh, and a mesh with no
    /// node, is one row of its own, so every fabric and every mesh reaches the caller in a frame.
    pub fn into_rows(self) -> Vec<Row> {
        match self {
            Self::Nodes(r) => r.into_iter().map(Row::Node).collect(),
            Self::Meshes(r) => r.into_iter().map(Row::Mesh).collect(),
            Self::Fabrics(r) => r.into_iter().map(Row::Fabric).collect(),
            Self::Connections(r) if r.connections.is_empty() => vec![Row::Connection { node: r.node, complete: r.complete, connection: None }],
            Self::Connections(r) => {
                let (node, complete) = (r.node, r.complete);
                r.connections.into_iter().map(|c| Row::Connection { node: node.clone(), complete, connection: Some(c) }).collect()
            }
            Self::Atlas(a) => {
                let mut rows = Vec::new();
                for mut fabric in a.fabrics {
                    let meshes = std::mem::take(&mut fabric.meshes);
                    if meshes.is_empty() {
                        rows.push(Row::Atlas { fabric, mesh: None, node: None });
                        continue;
                    }
                    for mut mesh in meshes {
                        let nodes = std::mem::take(&mut mesh.nodes);
                        if nodes.is_empty() {
                            rows.push(Row::Atlas { fabric: fabric.clone(), mesh: Some(mesh), node: None });
                            continue;
                        }
                        rows.extend(nodes.into_iter().map(|n| Row::Atlas { fabric: fabric.clone(), mesh: Some(mesh.clone()), node: Some(n) }));
                    }
                }
                rows
            }
        }
    }
}

/// One row of a read, of any entity: what the packer orders and measures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    /// A node row.
    Node(NodeView),
    /// A mesh row.
    Mesh(MeshView),
    /// A fabric row.
    Fabric(FabricView),
    /// A connection row of `(node, complete)`.
    Connection {
        /// The answering node's held `(path, name)`.
        node: String,
        /// Whether the node holds the complete set.
        complete: bool,
        /// The connection row, or `None` when the node holds no matching connection (so the stream
        /// still carries one `Connections` frame naming `node` and `complete`).
        connection: Option<ConnectionView>,
    },
    /// An atlas row of `(fabric, mesh)`.
    Atlas {
        /// The fabric, its meshes empty.
        fabric: AtlasFabricView,
        /// The mesh, its nodes empty; `None` for a fabric with no mesh.
        mesh: Option<AtlasMeshView>,
        /// The node row, or `None` for a fabric or a mesh with none (so the stream still carries one
        /// `Atlas` frame naming it).
        node: Option<AtlasNodeView>,
    },
}

impl Row {
    /// The row by its name, as a refusal names it: an atlas node row by its node_id.
    pub fn name(&self) -> String {
        match self {
            Self::Node(n) => n.node_id.to_string(),
            Self::Mesh(m) => m.name.clone(),
            Self::Fabric(f) => f.name.clone(),
            Self::Connection { connection: Some(c), .. } => c.id.clone(),
            Self::Connection { node, .. } => node.clone(),
            Self::Atlas { node: Some(n), .. } => n.node.node_id.to_string(),
            Self::Atlas { mesh: Some(m), .. } => m.name.clone(),
            Self::Atlas { fabric, .. } => fabric.name.clone(),
        }
    }

    /// Whether `next` goes in the same data frame as this row: a row of the same entity and, for a
    /// connection, the same `(node, complete)`, for an atlas row the same fabric and mesh. A batch is
    /// cut where this changes.
    pub fn shares_frame(&self, next: &Row) -> bool {
        match (self, next) {
            (Self::Node(_), Self::Node(_)) | (Self::Mesh(_), Self::Mesh(_)) | (Self::Fabric(_), Self::Fabric(_)) => true,
            (Self::Connection { node, complete, .. }, Self::Connection { node: next_node, complete: next_complete, .. }) => node == next_node && complete == next_complete,
            (Self::Atlas { fabric, mesh, .. }, Self::Atlas { fabric: next_fabric, mesh: next_mesh, .. }) => fabric.id == next_fabric.id && mesh.as_ref().map(|m| &m.name) == next_mesh.as_ref().map(|m| &m.name),
            _ => false,
        }
    }

    /// One data frame of `batch`, every row of one frame ([`Self::shares_frame`]).
    pub fn frame(batch: Vec<Row>) -> ReadReply {
        match batch.first() {
            Some(Row::Mesh(_)) => ReadReply::Meshes { meshes: batch.into_iter().filter_map(|r| if let Row::Mesh(m) = r { Some(m) } else { None }).collect() },
            Some(Row::Fabric(_)) => ReadReply::Fabrics { fabrics: batch.into_iter().filter_map(|r| if let Row::Fabric(f) = r { Some(f) } else { None }).collect() },
            Some(Row::Connection { node, complete, .. }) => {
                let (node, complete) = (node.clone(), *complete);
                ReadReply::Connections {
                    node,
                    complete,
                    connections: batch.into_iter().filter_map(|r| if let Row::Connection { connection: Some(c), .. } = r { Some(c) } else { None }).collect(),
                }
            }
            Some(Row::Atlas { fabric, mesh, .. }) => {
                let (fabric, mesh) = (fabric.clone(), mesh.clone());
                ReadReply::Atlas { fabric, mesh, nodes: batch.into_iter().filter_map(|r| if let Row::Atlas { node: Some(n), .. } = r { Some(n) } else { None }).collect() }
            }
            _ => ReadReply::Nodes { nodes: batch.into_iter().filter_map(|r| if let Row::Node(n) = r { Some(n) } else { None }).collect() },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: docs/architecture/node-rpc-envelope.md "Read, op `0x23`, rdm" (Q285): a stream, not
    /// forwardable; four requests, sixteen reply positions in the shown order; Started, the five data
    /// frames, End terminal, every other position a refusal.
    #[test]
    fn read_protocol_wire_positions_and_stream_classifications_match_contract() {
        assert_eq!(Read::OP, 0x23);
        assert_eq!(Read::NAME, "read");
        assert!(!Read::FORWARDABLE);
        assert_eq!(Read::REQUEST_VARIANTS, 4);
        assert_eq!(Read::REPLY_VARIANTS, 16);
        for (q, op, position) in [(ReadRequest::GetAtlas, "fabric.atlas.get", 2), (ReadRequest::GetMeshAtlas { filter: Filter::new().eq("name", ["mesh1"]) }, "mesh.atlas.get", 3)] {
            assert_eq!(q.op(), op);
            let b = Read::encode_request(&q).unwrap();
            assert_eq!(b[0], position);
            assert_eq!(Read::decode_request(&b).unwrap(), q);
        }
        let fabric = AtlasFabricView {
            id: crate::FabricId::mint(),
            name: "fabric1".into(),
            status: crate::views::ScopeStatus::Ready,
            provider: crate::views::ProviderKind::Process,
            fabric_primary: None,
            build_id: None,
            next_expiry: None,
            topology_storage_provider: None,
            previous_topology_storage_providers: Vec::new(),
            roots: None,
            ca: None,
            build: None,
            meshes: Vec::new(),
        };
        assert_eq!(Read::frame_kind(&ReadReply::Atlas { fabric, mesh: None, nodes: Vec::new() }), FrameKind::Data);
        let q = ReadRequest::Get { entity: Entity::Nodes, filter: Filter::new().eq("mesh", ["mesh1"]) };
        assert_eq!(q.op(), "node.get");
        let b = Read::encode_request(&q).unwrap();
        assert_eq!(b[0], 0);
        assert_eq!(Read::decode_request(&b).unwrap(), q);
        let qc = ReadRequest::GetConnections { node: NodeId::mint(), incarnation: IncarnationId::mint(), filter: Filter::new() };
        assert_eq!(qc.op(), "node.connection.get");
        let bc = Read::encode_request(&qc).unwrap();
        assert_eq!(bc[0], 1);
        assert_eq!(Read::decode_request(&bc).unwrap(), qc);
        for (frame, kind) in [
            (ReadReply::Started { read: "node.get".into() }, FrameKind::Started),
            (ReadReply::Nodes { nodes: Vec::new() }, FrameKind::Data),
            (ReadReply::Meshes { meshes: Vec::new() }, FrameKind::Data),
            (ReadReply::Fabrics { fabrics: Vec::new() }, FrameKind::Data),
            (ReadReply::Connections { node: "mesh1.node.1".into(), complete: true, connections: Vec::new() }, FrameKind::Data),
            (ReadReply::End { rows: 0, frames: 0 }, FrameKind::Terminal),
        ] {
            assert_eq!(Read::frame_kind(&frame), kind, "{frame:?}");
        }
        assert!(matches!(Read::frame_kind(&ReadReply::ReadOverBound { read: "node.get".into(), row: "mesh1.node.1".into(), bytes: 70_000, max: 65_536 }), FrameKind::Refusal(_)));
    }
}
