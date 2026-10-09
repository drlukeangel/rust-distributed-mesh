//! The wire shape of a membership digest, shared by every postcard codec that carries one: a
//! gossip frame (`rafka-mesh-transport` `wire`) and the join (`rafka-node-rpc-contract` `join`).
//!
//! `MeshDigest` is JSON elsewhere (REST, files, tests); its `skip_serializing_if` fields and its
//! runtime locator's internal tag cannot be read positionally. `WireDigest` carries the same
//! facts with every field present and the locator externally tagged.

use crate::runtime::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use crate::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeLoad, PathName};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

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

/// A [`MeshDigest`] as it travels in a gossip frame.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireDigest {
    fabric_id: FabricId,
    node: WireNode,
    status: MemberStatus,
    admin_api_base: Option<String>,
    digest_seq: u64,
    emitted_at_rafka_ms: u64,
    data_dir: Option<String>,
    mesh_id: Option<MeshId>,
    in_flight: Option<u64>,
    extra: BTreeMap<String, String>,
    load: Option<NodeLoad>,
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
            status: d.status,
            admin_api_base: d.admin_api_base.clone(),
            digest_seq: d.digest_seq,
            emitted_at_rafka_ms: d.emitted_at_rafka_ms,
            data_dir: d.data_dir.clone(),
            mesh_id: d.mesh_id.clone(),
            in_flight: d.in_flight,
            extra: d.extra.clone(),
            load: d.load,
        }
    }
}

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
            status: w.status,
            admin_api_base: w.admin_api_base,
            digest_seq: w.digest_seq,
            emitted_at_rafka_ms: w.emitted_at_rafka_ms,
            data_dir: w.data_dir,
            mesh_id: w.mesh_id,
            in_flight: w.in_flight,
            extra: w.extra,
            load: w.load,
        }
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

/// `#[serde(with = "crate::wire::digests")]`: a run of digests in a frame.
pub mod digests {
    use super::*;
    /// Serialize digests in their wire shape.
    pub fn serialize<S: Serializer>(ds: &[MeshDigest], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(ds.iter().map(WireDigest::from))
    }
    /// Deserialize digests from their wire shape.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<MeshDigest>, D::Error> {
        Vec::<WireDigest>::deserialize(d).map(|v| v.into_iter().map(Into::into).collect())
    }
}
