//! The wire shape of a topology digest, shared by every postcard codec that carries one: a
//! gossip frame (`rafka-mesh-transport` `wire`) and the join (`rafka-node-rpc-contract` `join`).
//!
//! `MeshDigest` is JSON elsewhere (REST, files, tests); its `skip_serializing_if` fields and its
//! runtime locator's internal tag cannot be read positionally. `WireDigest` carries the same
//! facts with every field present and the locator externally tagged.
//!
//! A heartbeat is the digest alone: liveness (`node_state`), endpoints and the runtime fact, and no
//! status (gossip.md §3.1 `Digest`: "it carries no status: a member's status comes only from its
//! mesh-primary"). A mesh-primary's member projection (`MeshTopology`, `MeshTopologyDelta`, the topology
//! read) carries each member as a [`ProjectionEntry`]: its digest, then the status the primary holds.

use crate::runtime::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use crate::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeLoad, NodeState, GossipStats, PathName};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum WireLocator {
    Process { pid: u32, start: u64 },
    Container { id: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct WireRuntime {
    deployment_id: String,
    provider: RuntimeProvider,
    control_domain: String,
    locator: WireLocator,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct WireNode {
    node_id: NodeId,
    name: PathName,
    endpoint_id: EndpointId,
    transport_addr: std::net::SocketAddr,
    incarnation: IncarnationId,
    supersedes: Option<IncarnationId>,
    runtime: Option<WireRuntime>,
}

/// A [`MeshDigest`] as it travels in a gossip frame: position 2 is the self-published liveness
/// `node_state`; no status travels with it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireDigest {
    fabric_id: FabricId,
    node: WireNode,
    node_state: NodeState,
    admin_api_base: Option<String>,
    digest_seq: u64,
    emitted_at_rafka_ms: u64,
    data_dir: Option<String>,
    mesh_id: Option<MeshId>,
    in_flight: Option<u64>,
    load: Option<NodeLoad>,
    gossip: Option<GossipStats>,
    held_hash: [u8; 32],
    is_primary: Option<u64>,
    is_fabric_primary: Option<u64>,
}

impl From<&MeshDigest> for WireDigest {
    fn from(d: &MeshDigest) -> Self {
        let n = &d.node;
        let runtime = n.runtime.as_ref().map(|r| WireRuntime {
            deployment_id: r.deployment_id.clone(),
            provider: r.provider,
            control_domain: r.control_domain.clone(),
            locator: match &r.locator {
                RuntimeLocator::Process { pid, start } => WireLocator::Process { pid: *pid, start: *start },
                RuntimeLocator::Container { id } => WireLocator::Container { id: id.clone() },
            },
        });
        Self {
            fabric_id: d.fabric_id.clone(),
            node: WireNode {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                endpoint_id: n.endpoint_id.clone(),
                transport_addr: n.transport_addr,
                incarnation: n.incarnation.clone(),
                supersedes: n.supersedes.clone(),
                runtime,
            },
            node_state: d.node_state,
            admin_api_base: d.admin_api_base.clone(),
            digest_seq: d.digest_seq,
            emitted_at_rafka_ms: d.emitted_at_rafka_ms,
            data_dir: d.data_dir.clone(),
            mesh_id: d.mesh_id.clone(),
            in_flight: d.in_flight,
            load: d.load,
            gossip: d.gossip,
            held_hash: d.held_hash,
            is_primary: d.is_primary,
            is_fabric_primary: d.is_fabric_primary,
        }
    }
}

/// A heartbeat decoded: it carries no status, so the digest holds `Pending`, the status of a birth
/// whose status its holder was not told; the holder's view gives it the status it holds for the
/// birth. A projection entry carries the status ([`ProjectionEntry`]).
impl From<WireDigest> for MeshDigest {
    fn from(w: WireDigest) -> Self {
        let n = w.node;
        let runtime = n.runtime.map(|r| RuntimeFact {
            deployment_id: r.deployment_id,
            provider: r.provider,
            control_domain: r.control_domain,
            locator: match r.locator {
                WireLocator::Process { pid, start } => RuntimeLocator::Process { pid, start },
                WireLocator::Container { id } => RuntimeLocator::Container { id },
            },
        });
        MeshDigest {
            fabric_id: w.fabric_id,
            node: MeshNode {
                node_id: n.node_id,
                name: n.name,
                endpoint_id: n.endpoint_id,
                transport_addr: n.transport_addr,
                incarnation: n.incarnation,
                supersedes: n.supersedes,
                runtime,
            },
            status: MemberStatus::Pending,
            node_state: w.node_state,
            admin_api_base: w.admin_api_base,
            digest_seq: w.digest_seq,
            emitted_at_rafka_ms: w.emitted_at_rafka_ms,
            data_dir: w.data_dir,
            mesh_id: w.mesh_id,
            in_flight: w.in_flight,
            load: w.load,
            gossip: w.gossip,
            held_hash: w.held_hash,
            is_primary: w.is_primary,
            is_fabric_primary: w.is_fabric_primary,
        }
    }
}

/// One member of a mesh-primary's member projection as it travels (gossip.md §3.1 `MeshTopology`,
/// `MeshTopologyDelta`; the topology read): its digest (liveness, endpoints, runtime), then its status,
/// which the primary sets. Postcard-positional: `digest` at position 0, `status` at 1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ProjectionEntry {
    /// The member's digest.
    pub digest: WireDigest,
    /// The member's status, as its mesh-primary holds it.
    pub status: MemberStatus,
}

impl From<&MeshDigest> for ProjectionEntry {
    fn from(d: &MeshDigest) -> Self {
        Self { digest: WireDigest::from(d), status: d.status }
    }
}

impl From<ProjectionEntry> for MeshDigest {
    fn from(e: ProjectionEntry) -> Self {
        MeshDigest { status: e.status, ..MeshDigest::from(e.digest) }
    }
}

/// `#[serde(with = "crate::wire::entries")]`: a run of member projection entries in a frame.
pub mod entries {
    use super::*;
    /// Serialize members as projection entries.
    pub fn serialize<S: Serializer>(ds: &[MeshDigest], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(ds.iter().map(ProjectionEntry::from))
    }
    /// Deserialize members from projection entries.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<MeshDigest>, D::Error> {
        Vec::<ProjectionEntry>::deserialize(d).map(|v| v.into_iter().map(Into::into).collect())
    }
}

/// `#[serde(with = "crate::wire::digest")]`: one digest in a frame.
pub mod digest {
    use super::*;
    /// Serialize a digest in its wire shape.
    pub fn serialize<S: Serializer>(d: &MeshDigest, s: S) -> Result<S::Ok, S::Error> {
        WireDigest::from(d).serialize(s)
    }
    /// Deserialize a digest from its wire shape.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<MeshDigest, D::Error> {
        WireDigest::deserialize(d).map(Into::into)
    }
}

/// `#[serde(with = "crate::wire::runtime")]`: one runtime fact in a postcard message. The fact's
/// locator is internally tagged for JSON; this shape carries it externally tagged, positionally.
pub mod runtime {
    use super::*;
    /// Serialize a runtime fact in its wire shape.
    pub fn serialize<S: Serializer>(r: &RuntimeFact, s: S) -> Result<S::Ok, S::Error> {
        WireRuntime {
            deployment_id: r.deployment_id.clone(),
            provider: r.provider,
            control_domain: r.control_domain.clone(),
            locator: match &r.locator {
                RuntimeLocator::Process { pid, start } => WireLocator::Process { pid: *pid, start: *start },
                RuntimeLocator::Container { id } => WireLocator::Container { id: id.clone() },
            },
        }
        .serialize(s)
    }
    /// Deserialize a runtime fact from its wire shape.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<RuntimeFact, D::Error> {
        let r = WireRuntime::deserialize(d)?;
        Ok(RuntimeFact {
            deployment_id: r.deployment_id,
            provider: r.provider,
            control_domain: r.control_domain,
            locator: match r.locator {
                WireLocator::Process { pid, start } => RuntimeLocator::Process { pid, start },
                WireLocator::Container { id } => RuntimeLocator::Container { id },
            },
        })
    }
}
