//! The positional wire shapes of a node, shared by every postcard codec that carries one: the gossip
//! frames (`rdm-gossip` `frame`), the join (`join`) and the topology read (`topology`).
//!
//! `MeshDigest` is JSON elsewhere (REST, files, tests); its `skip_serializing_if` fields and a runtime
//! locator's internal tag cannot be read positionally. These shapes carry the same facts with every
//! field present and the locator externally tagged.
//!
//! A Digest carries the node's status and the primary roles it holds; its first publication into its
//! mesh also carries its [`NodeDetails`] (gossip.md §3.1 `Digest`). The author, sequence and time of a
//! gossip frame are its envelope's, never its body's.

use crate::runtime::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use crate::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshNode, NodeId, RoleRevision};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum WireLocator {
    /// Position 0 on the wire.
    Process { pid: u32, start: u64 },
    /// Position 1 on the wire.
    Container { id: String },
}

/// A [`RuntimeFact`] in its positional shape, its locator externally tagged.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireRuntime {
    deployment_id: String,
    provider: RuntimeProvider,
    control_domain: String,
    locator: WireLocator,
}

impl From<&RuntimeFact> for WireRuntime {
    fn from(r: &RuntimeFact) -> Self {
        Self {
            deployment_id: r.deployment_id.clone(),
            provider: r.provider,
            control_domain: r.control_domain.clone(),
            locator: match &r.locator {
                RuntimeLocator::Process { pid, start } => WireLocator::Process { pid: *pid, start: *start },
                RuntimeLocator::Container { id } => WireLocator::Container { id: id.clone() },
            },
        }
    }
}

impl From<WireRuntime> for RuntimeFact {
    fn from(r: WireRuntime) -> Self {
        RuntimeFact {
            deployment_id: r.deployment_id,
            provider: r.provider,
            control_domain: r.control_domain,
            locator: match r.locator {
                WireLocator::Process { pid, start } => RuntimeLocator::Process { pid, start },
                WireLocator::Container { id } => RuntimeLocator::Container { id },
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct WireNode {
    mesh: String,
    r#type: String,
    node_id: NodeId,
    path: String,
    /// The name, beside its path.
    name: String,
    endpoint_id: EndpointId,
    transport_addr: std::net::SocketAddr,
    incarnation: IncarnationId,
    supersedes: Option<IncarnationId>,
}

/// A [`MeshDigest`] in its positional shape: what the join report carries of the joining birth.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireDigest {
    fabric_id: FabricId,
    node: WireNode,
    status: NodeStatus,
    primary: Vec<RoleRevision>,
    digest_seq: u64,
    emitted_at_rafka_ms: u64,
}

impl From<&MeshDigest> for WireDigest {
    fn from(d: &MeshDigest) -> Self {
        let n = &d.node;
        Self {
            fabric_id: d.fabric_id.clone(),
            node: WireNode {
                mesh: n.mesh.clone(),
                r#type: n.r#type.clone(),
                node_id: n.node_id.clone(),
                path: n.path.clone(), name: n.name.clone(),
                endpoint_id: n.endpoint_id.clone(),
                transport_addr: n.transport_addr,
                incarnation: n.incarnation.clone(),
                supersedes: n.supersedes.clone(),
            },
            status: d.status,
            primary: d.primary.clone(),
            digest_seq: d.digest_seq,
            emitted_at_rafka_ms: d.emitted_at_rafka_ms,
        }
    }
}

impl From<WireDigest> for MeshDigest {
    fn from(w: WireDigest) -> Self {
        let n = w.node;
        MeshDigest {
            fabric_id: w.fabric_id,
            node: MeshNode { mesh: n.mesh, r#type: n.r#type,
                node_id: n.node_id,
                path: n.path, name: n.name,
                endpoint_id: n.endpoint_id,
                transport_addr: n.transport_addr,
                incarnation: n.incarnation,
                supersedes: n.supersedes,
            },
            status: w.status,
            primary: w.primary,
            digest_seq: w.digest_seq,
            emitted_at_rafka_ms: w.emitted_at_rafka_ms,
        }
    }
}

/// A birth's node details as every Digest of it carries them (gossip.md §3.1 `Digest`): its type,
/// path, name, endpoint and lineage. The envelope's `source` names its fabric, node id and
/// incarnation; its mesh is the mesh channel's it is heard on.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeDetails {
    /// The node's type.
    pub r#type: String,
    /// The node's path.
    pub path: String,
    /// The node's name, beside its path.
    pub name: String,
    /// The birth's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
}

impl NodeDetails {
    /// The details of `node`.
    pub fn of(node: &MeshNode) -> Self {
        Self { r#type: node.r#type.clone(), path: node.path.clone(), name: node.name.clone(), endpoint_id: node.endpoint_id.clone(), transport_addr: node.transport_addr, supersedes: node.supersedes.clone() }
    }

    /// The birth `node_id`, `incarnation` of `mesh` (the mesh channel it is heard on) these details
    /// describe.
    pub fn node(&self, node_id: NodeId, incarnation: IncarnationId, mesh: &str) -> MeshNode {
        MeshNode {
            mesh: mesh.to_string(),
            r#type: self.r#type.clone(),
            node_id,
            path: self.path.clone(),
            name: self.name.clone(),
            endpoint_id: self.endpoint_id.clone(),
            transport_addr: self.transport_addr,
            incarnation,
            supersedes: self.supersedes.clone(),
        }
    }
}

/// One node as a mesh topology lists it (gossip.md §3.1): its identity, details, status and the
/// primary roles it holds. The aggregate lists every node of its mesh this way; a node under a stop or
/// delete, or deleted, says so by its status (Q390). A differential's `added` lists a node this way: a
/// node new to the mesh, or a held birth whose details changed. Its fabric is the envelope's and its
/// mesh the frame's.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeEntry {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The node's type.
    pub r#type: String,
    /// The node's path.
    pub path: String,
    /// The node's name, beside its path.
    pub name: String,
    /// The birth's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The node's status.
    pub status: NodeStatus,
    /// The primary roles the node holds, each with its revision.
    pub primary: Vec<RoleRevision>,
}

impl NodeEntry {
    /// `d` as a mesh topology lists it.
    pub fn of(d: &MeshDigest) -> Self {
        let n = &d.node;
        Self {
            node_id: n.node_id.clone(), incarnation: n.incarnation.clone(), r#type: n.r#type.clone(),
            path: n.path.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.clone(), transport_addr: n.transport_addr,
            status: d.status, primary: d.primary.clone(),
        }
    }

    /// The node of `mesh` (the frame names it) of fabric `fabric` (the envelope names it), as a
    /// receiver holds it: no Digest sequence or time, which only the node's own Digests carry.
    pub fn digest(&self, fabric: &FabricId, mesh: &str) -> MeshDigest {
        MeshDigest {
            fabric_id: fabric.clone(),
            node: MeshNode {
                mesh: mesh.to_string(), r#type: self.r#type.clone(), node_id: self.node_id.clone(),
                path: self.path.clone(), name: self.name.clone(), endpoint_id: self.endpoint_id.clone(),
                transport_addr: self.transport_addr, incarnation: self.incarnation.clone(), supersedes: None,
            },
            status: self.status,
            primary: self.primary.clone(),
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
        }
    }
}

/// A held node's status change as a differential carries it (gossip.md §3.1 `TopologyDelta`): the
/// exact birth, its status and the primary roles it holds; its details are the ones held.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeStatusEntry {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The node's status.
    pub status: NodeStatus,
    /// The primary roles the node holds, each with its revision.
    pub primary: Vec<RoleRevision>,
}

impl NodeStatusEntry {
    /// `d`'s status and primary roles.
    pub fn of(d: &MeshDigest) -> Self {
        Self { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), status: d.status, primary: d.primary.clone() }
    }
}

/// One node as a topology read serves it (gossip.md §3.3: reads initialise and repair views): its
/// identity, endpoint and lineage, its status and primary roles, the sequence of its Digest the
/// answerer holds (an older read never replaces a newer self-authored status), and the runtime and
/// data dir a node-admin manages it with (hydration; gossip never carries them). Its fabric is the
/// reader's and its mesh its path's.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReadNode {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The node's type.
    pub r#type: String,
    /// The node's path.
    pub path: String,
    /// The node's name, beside its path.
    pub name: String,
    /// The birth's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
    /// The node's status.
    pub status: NodeStatus,
    /// The primary roles the node holds, each with its revision.
    pub primary: Vec<RoleRevision>,
    /// The sequence of the node's Digest the answerer holds.
    pub digest_seq: u64,
    /// The birth's exact runtime, when the answerer holds it.
    runtime: Option<WireRuntime>,
    /// Where the birth keeps its data, when the answerer holds it.
    pub data_dir: Option<String>,
}

impl ReadNode {
    /// `d` as a read serves it, with the runtime and data dir `r` the answerer holds of it.
    pub fn of(d: &MeshDigest, r: Option<&crate::NodeRuntime>) -> Self {
        Self::of_node(&d.node, d.status, d.primary.clone(), d.digest_seq, r)
    }

    /// The birth `n` as a read serves it, with its `status`, the `primary` roles it holds, the
    /// `digest_seq` the answerer holds and the runtime and data dir `r` the answerer holds of it: the one
    /// row, whether the answerer holds the node as a digest ([`Self::of`]) or reads it from its record
    /// (the atlas, query.md §1).
    pub fn of_node(n: &MeshNode, status: NodeStatus, primary: Vec<RoleRevision>, digest_seq: u64, r: Option<&crate::NodeRuntime>) -> Self {
        Self {
            node_id: n.node_id.clone(), incarnation: n.incarnation.clone(), r#type: n.r#type.clone(),
            path: n.path.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.clone(), transport_addr: n.transport_addr,
            supersedes: n.supersedes.clone(), status, primary, digest_seq,
            runtime: r.and_then(|r| r.runtime.as_ref()).map(WireRuntime::from),
            data_dir: r.and_then(|r| r.data_dir.clone()),
        }
    }

    /// The birth, of `mesh` (the read names it).
    pub fn node(&self, mesh: &str) -> MeshNode {
        MeshNode {
            mesh: mesh.to_string(), r#type: self.r#type.clone(), node_id: self.node_id.clone(),
            path: self.path.clone(), name: self.name.clone(), endpoint_id: self.endpoint_id.clone(),
            transport_addr: self.transport_addr, incarnation: self.incarnation.clone(), supersedes: self.supersedes.clone(),
        }
    }

    /// The node of `mesh` (the read names it) as the reader of fabric `fabric` holds it.
    pub fn digest(&self, fabric: &FabricId, mesh: &str) -> MeshDigest {
        MeshDigest {
            fabric_id: fabric.clone(),
            node: self.node(mesh),
            status: self.status,
            primary: self.primary.clone(),
            digest_seq: self.digest_seq,
            emitted_at_rafka_ms: 0,
        }
    }

    /// The runtime and data dir the read carried of the node.
    pub fn runtime(&self) -> crate::NodeRuntime {
        crate::NodeRuntime { runtime: self.runtime.clone().map(Into::into), data_dir: self.data_dir.clone() }
    }
}

/// `#[serde(with = "crate::wire::entries")]`: a run of member projection entries in a frame.
pub mod entries {
    use super::*;
    /// Serialize members as projection entries.
    pub fn serialize<S: Serializer>(ds: &[MeshDigest], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(ds.iter().map(WireDigest::from))
    }
    /// Deserialize members from projection entries.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<MeshDigest>, D::Error> {
        Vec::<WireDigest>::deserialize(d).map(|v| v.into_iter().map(Into::into).collect())
    }
}

/// `#[serde(with = "crate::wire::runtime")]`: one runtime fact in a postcard message. The fact's
/// locator is internally tagged for JSON; this shape carries it externally tagged, positionally.
pub mod runtime {
    use super::*;
    /// Serialize a runtime fact in its wire shape.
    pub fn serialize<S: Serializer>(r: &RuntimeFact, s: S) -> Result<S::Ok, S::Error> {
        WireRuntime::from(r).serialize(s)
    }
    /// Deserialize a runtime fact from its wire shape.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<RuntimeFact, D::Error> {
        WireRuntime::deserialize(d).map(Into::into)
    }
}

/// `#[serde(with = "crate::wire::runtime_opt")]`: an optional runtime fact in a postcard message.
pub mod runtime_opt {
    use super::*;
    /// Serialize an optional runtime fact in its wire shape.
    pub fn serialize<S: Serializer>(r: &Option<RuntimeFact>, s: S) -> Result<S::Ok, S::Error> {
        r.as_ref().map(WireRuntime::from).serialize(s)
    }
    /// Deserialize an optional runtime fact from its wire shape.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<RuntimeFact>, D::Error> {
        Option::<WireRuntime>::deserialize(d).map(|r| r.map(Into::into))
    }
}
