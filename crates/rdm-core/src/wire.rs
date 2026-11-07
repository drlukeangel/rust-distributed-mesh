//! The wire shape of a topology digest, shared by every postcard codec that carries one: a
//! gossip frame (`rafka-mesh-transport` `wire`) and the join (`rafka-node-rpc-contract` `join`).
//!
//! `MeshDigest` is JSON elsewhere (REST, files, tests); its `skip_serializing_if` fields and its
//! runtime locator's internal tag cannot be read positionally. `WireDigest` carries the same
//! facts with every field present and the locator externally tagged.
//!
//! A Digest carries the node's status (gossip.md §3.1 `Digest`), and so does every frame that carries
//! a node: a mesh-primary's `TopologyFull`, `TopologyDelta` and the topology read carry each node as
//! its Digest, status included.

use crate::runtime::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use crate::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeLoad, GossipStats};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum WireLocator {
    /// Position 0 on the wire.
    Process { pid: u32, start: u64 },
    /// Position 1 on the wire.
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
    runtime: Option<WireRuntime>,
}

/// A [`MeshDigest`] as it travels in a gossip frame: position 2 is the node's status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireDigest {
    fabric_id: FabricId,
    node: WireNode,
    status: NodeStatus,
    admin_api_base: Option<String>,
    digest_seq: u64,
    emitted_at_rafka_ms: u64,
    data_dir: Option<String>,
    mesh_id: Option<MeshId>,
    in_flight: Option<u64>,
    load: Option<NodeLoad>,
    gossip: Option<GossipStats>,
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
                mesh: n.mesh.clone(),
                r#type: n.r#type.clone(),
                node_id: n.node_id.clone(),
                path: n.path.clone(), name: n.name.clone(),
                endpoint_id: n.endpoint_id.clone(),
                transport_addr: n.transport_addr,
                incarnation: n.incarnation.clone(),
                supersedes: n.supersedes.clone(),
                runtime,
            },
            status: d.status,
            admin_api_base: d.admin_api_base.clone(),
            digest_seq: d.digest_seq,
            emitted_at_rafka_ms: d.emitted_at_rafka_ms,
            data_dir: d.data_dir.clone(),
            mesh_id: d.mesh_id.clone(),
            in_flight: d.in_flight,
            load: d.load,
            gossip: d.gossip,
            is_primary: d.is_primary,
            is_fabric_primary: d.is_fabric_primary,
        }
    }
}

/// A Digest decoded, with the status it carried.
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
            node: MeshNode { mesh: n.mesh, r#type: n.r#type.clone(),
                node_id: n.node_id,
                path: n.path.clone(), name: n.name.clone(),
                endpoint_id: n.endpoint_id,
                transport_addr: n.transport_addr,
                incarnation: n.incarnation,
                supersedes: n.supersedes,
                runtime,
            },
            status: w.status,
            admin_api_base: w.admin_api_base,
            digest_seq: w.digest_seq,
            emitted_at_rafka_ms: w.emitted_at_rafka_ms,
            data_dir: w.data_dir,
            mesh_id: w.mesh_id,
            in_flight: w.in_flight,
            load: w.load,
            gossip: w.gossip,
            is_primary: w.is_primary,
            is_fabric_primary: w.is_fabric_primary,
        }
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
